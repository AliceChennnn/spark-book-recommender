"""Explicit CSV-field adapter. All Spark work happens outside the WebUI."""

from dataclasses import dataclass

try:
    from .datasets import DatasetSpec
except ImportError:
    from datasets import DatasetSpec


@dataclass
class IdMappings:
    dataset_id: str
    mapping_version: str
    users: object
    books: object


@dataclass
class AdaptedDataset:
    spec: DatasetSpec
    ratings: object
    books: object
    mappings: IdMappings

    def summary(self):
        from pyspark.sql import functions as F
        stats = self.ratings.agg(F.count("*").alias("ratings"),
            F.countDistinct("user_id").alias("users"),
            F.min("rating").alias("min_rating"), F.max("rating").alias("max_rating")).first().asDict()
        return dict(stats, books=self.books.count(), topics_available=False,
                    topic_message="该数据集未提供主题标签，主题画像暂不可用。")

    def preview_popularity(self, user_id, top_k=10):
        """Unsplit observed-count preview, never an official train-only baseline."""
        from pyspark.sql import functions as F
        if type(user_id) is not int or not 1 <= user_id <= 2147483647:
            raise ValueError("user_id must be a positive internal INT")
        if type(top_k) is not int or not 1 <= top_k <= 50:
            raise ValueError("top_k must be from 1 to 50")
        seen = self.ratings.filter(F.col("user_id") == user_id).select("book_id")
        counts = self.ratings.groupBy("book_id").agg(F.count("*").alias("observed_rating_count"))
        rows = (self.books.join(seen, "book_id", "left_anti").join(counts, "book_id", "left")
                .fillna({"observed_rating_count": 0})
                .orderBy(F.desc("observed_rating_count"), "book_id").limit(top_k).collect())
        return {"binding": self.spec.data_binding, "algorithm": "observed_popularity_preview",
                "evaluation_scope": "all observed ratings; no train/test split or ALS model",
                "topics_available": False, "recommendations": [row.asDict() for row in rows]}


def _reject(frame, condition, message):
    if frame.filter(condition).limit(1).count():
        raise ValueError(message)


def _unique(frame, columns, label):
    from pyspark.sql import functions as F
    if frame.groupBy(*columns).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Duplicate {label}; declare a source-specific policy before ingestion")


def _mapping(ids, internal, previous=None):
    from pyspark.sql import Window, functions as F
    from pyspark.sql.types import IntegerType, StringType
    if previous is None:
        previous = ids.sparkSession.createDataFrame([], f"source_id STRING, {internal} INT")
    if (set(previous.columns) != {"source_id", internal}
            or previous.schema["source_id"].dataType != StringType()
            or previous.schema[internal].dataType != IntegerType()):
        raise ValueError("Persisted mappings must contain source_id STRING and an internal INT")
    _reject(previous, F.col("source_id").isNull() | (F.length(F.trim("source_id")) == 0)
            | F.col(internal).isNull() | (F.col(internal) <= 0), "Invalid persisted ID mapping")
    _unique(previous, ["source_id"], "source mapping ID")
    _unique(previous, [internal], "internal mapping ID")
    maximum = previous.agg(F.max(internal)).first()[0] or 0
    fresh = ids.distinct().join(previous.select("source_id"), "source_id", "left_anti")
    if maximum + fresh.count() > 2147483647:
        raise ValueError("Internal ID mapping would exceed INT capacity")
    added = fresh.withColumn(internal,
        (F.row_number().over(Window.orderBy("source_id")) + F.lit(maximum)).cast("int"))
    return previous.select("source_id", internal).unionByName(added.select("source_id", internal))


def adapt_frames(ratings, books, spec, previous=None):
    """Reject ambiguity before mapping; preserve exact source strings, including leading zeros."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    if previous is not None and (previous.dataset_id, previous.mapping_version) != (
            spec.dataset_id, spec.mapping_version):
        raise ValueError("Cannot reuse ID mappings across datasets or mapping versions")
    for frame, fields in ((ratings, spec.ratings_columns), (books, spec.books_columns)):
        for source in fields.values():
            if source is not None and source not in frame.columns:
                raise ValueError(f"Missing declared source column: {source}")
        id_fields = [source for name, source in fields.items() if name in ("user_id", "book_id")]
        if any(frame.schema[name].dataType != StringType() for name in id_fields):
            raise ValueError("Source IDs must be STRING; never cast arbitrary IDs to INT")
        for name in ("title", "authors"):
            source = fields.get(name)
            if source is not None and frame.schema[source].dataType != StringType():
                raise ValueError(f"{name} must be a STRING source column")
    r = ratings.select(*[F.col(spec.ratings_columns[name]).alias(alias) for name, alias in (
        ("user_id", "source_user_id"), ("book_id", "source_book_id"), ("rating", "source_rating"))])
    b = books.select(F.col(spec.books_columns["book_id"]).alias("source_book_id"),
        F.col(spec.books_columns["title"]).alias("title"),
        (F.col(spec.books_columns["authors"]) if spec.books_columns["authors"] else F.lit(None))
        .cast("string").alias("authors"))
    for frame, names in ((r, ("source_user_id", "source_book_id")), (b, ("source_book_id",))):
        for name in names:
            _reject(frame, F.col(name).isNull() | (F.length(F.trim(F.col(name))) == 0),
                    f"Missing or empty source ID: {name}")
    if not r.limit(1).count() or not b.limit(1).count():
        raise ValueError("Ratings and books must both be nonempty")
    _reject(b, F.col("title").isNull() | (F.length(F.trim("title")) == 0), "Missing book title")
    _unique(r, ["source_user_id", "source_book_id"], "rating key")
    _unique(b, ["source_book_id"], "book ID")
    if r.join(b, "source_book_id", "left_anti").limit(1).count():
        raise ValueError("Ratings reference books absent from the metadata")
    r = r.withColumn("source_rating", F.col("source_rating").cast("double"))
    scale = spec.rating_scale
    _reject(r, F.col("source_rating").isNull() | F.isnan("source_rating")
            | (F.col("source_rating") < scale.source_min) | (F.col("source_rating") > scale.source_max),
            "Invalid explicit rating or rating outside the declared source range")
    users = _mapping(r.select(F.col("source_user_id").alias("source_id")), "user_id",
                     previous.users if previous is not None else None)
    book_map = _mapping(b.select(F.col("source_book_id").alias("source_id")), "book_id",
                        previous.books if previous is not None else None)
    mapped = r.join(users.withColumnRenamed("source_id", "source_user_id"), "source_user_id")
    mapped = mapped.join(book_map.withColumnRenamed("source_id", "source_book_id"), "source_book_id")
    value = F.col("source_rating")
    if scale.transform == "linear":
        value = (value - scale.source_min) / (scale.source_max - scale.source_min)
        value = value * (scale.target_max - scale.target_min) + scale.target_min
    mapped = mapped.select("user_id", "book_id", value.cast("double").alias("rating"))
    metadata = b.join(book_map.withColumnRenamed("source_id", "source_book_id"), "source_book_id")
    return AdaptedDataset(spec, mapped, metadata.select("book_id", "title", "authors"),
                          IdMappings(spec.dataset_id, spec.mapping_version, users, book_map))
