"""Build general shelf features and a conservative subset of topic features."""

import argparse
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StructField, StructType


HDFS_ROOT = "hdfs://master:9000/user/root/bookrec"
POLICY_VERSION = "goodbooks-topics-v1"

# Exact names only: a shelf is not necessarily a genre. Aliases remain separate
# tag IDs in this phase; an unlisted name is not evidence of a non-topic shelf.
TOPIC_TAGS = frozenset({
    "fiction", "non-fiction", "nonfiction", "fantasy", "science-fiction",
    "sci-fi", "scifi", "science-fiction-fantasy", "science-fiction-and-fantasy",
    "mystery", "mystery-thriller", "thriller", "thrillers", "suspense",
    "crime", "detective", "horror", "romance", "historical-fiction",
    "historical-romance", "history", "biography", "biographies", "memoir",
    "memoirs", "autobiography", "classics", "classic", "literature",
    "contemporary", "contemporary-fiction", "literary-fiction", "young-adult",
    "ya", "children", "childrens", "children-s", "children-s-books",
    "middle-grade", "picture-books", "adventure", "dystopia", "dystopian",
    "paranormal", "paranormal-romance", "urban-fantasy", "epic-fantasy",
    "humor", "humour", "comedy", "poetry", "plays", "drama", "short-stories",
    "essays", "comics", "graphic-novels", "manga", "science", "psychology",
    "philosophy", "religion", "spirituality", "politics", "economics",
    "business", "self-help", "travel", "art", "music", "cookbooks", "cooking",
    "health", "education", "true-crime", "war", "western", "westerns",
})

BOOK_TAG_SCHEMA = StructType([
    StructField("goodreads_book_id", LongType(), True),
    StructField("tag_id", IntegerType(), True),
    StructField("count", LongType(), True),
])


def normalize_tag_name(column):
    """Normalize spelling separators without merging tag identities."""
    return F.regexp_replace(F.lower(F.trim(column)), r"[\s_]+", "-")


def valid_book_tags(frame):
    return frame.filter(
        (F.col("goodreads_book_id") > 0)
        & (F.col("tag_id") >= 0)
        & (F.col("count") > 0)
    )


def deduplicate_book_tags(frame):
    """Counts are shelf totals, so repeated observations use max, not sum."""
    return frame.groupBy("goodreads_book_id", "tag_id").agg(
        F.max("count").alias("tag_count")
    )


def join_book_tags(book_tags, books, tags):
    # ratings/ALS use book_id; shelf data use Goodreads edition IDs.
    return (
        book_tags.join(books, "goodreads_book_id", "inner")
        .join(tags, "tag_id", "inner")
        .select("book_id", "tag_id", "tag_name", "tag_count")
    )


def select_topics(frame):
    return frame.filter(F.col("tag_name").isin(sorted(TOPIC_TAGS)))


def normalize_weights(frame):
    total = F.sum("tag_count").over(Window.partitionBy("book_id"))
    return frame.withColumn("tag_weight", F.col("tag_count") / total).select(
        "book_id", "tag_id", "tag_name", "tag_count", "tag_weight"
    )


def quality_summary(frame):
    """Validate persisted features, including uniqueness and per-book L1 sums."""
    stats = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct("book_id").alias("books"),
        F.countDistinct("book_id", "tag_id").alias("unique_pairs"),
        F.sum(F.when(
            F.col("book_id").isNull() | (F.col("book_id") <= 0)
            | F.col("tag_id").isNull() | (F.col("tag_id") < 0)
            | F.col("tag_name").isNull() | (F.length("tag_name") == 0)
            | F.col("tag_count").isNull() | (F.col("tag_count") <= 0)
            | F.col("tag_weight").isNull() | F.isnan("tag_weight")
            | (F.col("tag_weight") <= 0) | (F.col("tag_weight") > 1), 1
        ).otherwise(0)).alias("invalid_rows"),
    ).first().asDict()
    error = frame.groupBy("book_id").agg(
        F.abs(F.sum("tag_weight") - 1.0).alias("error")
    ).agg(F.max("error")).first()[0]
    stats["max_weight_sum_error"] = error
    if (stats["rows"] == 0 or stats["rows"] != stats["unique_pairs"]
            or stats["invalid_rows"] or error is None or error > 1e-9):
        raise ValueError(f"Invalid tag features: {stats}")
    return stats


def require_unique(frame, key, label):
    if frame.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Ambiguous {label}: duplicate {key}")


def ensure_new_output(spark, output_root):
    path = spark._jvm.org.apache.hadoop.fs.Path(output_root)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path):
        raise FileExistsError(f"Output already exists; refusing to overwrite: {output_root}")


def prepare_tags(spark, hdfs_root, output_root):
    ensure_new_output(spark, output_root)
    cached = []

    def keep(frame):
        cached.append(frame.cache())
        return cached[-1]

    report = {"topic_policy": POLICY_VERSION,
              "topic_allowlist": sorted(TOPIC_TAGS),
              "deduplication": "max positive count per (goodreads_book_id, tag_id)",
              "weighting": "tag_count / sum(tag_count) per book, separately per feature set",
              "hdfs_root": hdfs_root, "output_root": output_root}
    try:
        raw = keep(spark.read.option("header", True).option("mode", "FAILFAST")
                   .schema(BOOK_TAG_SCHEMA).csv(f"{hdfs_root}/raw/book_tags.csv"))
        raw_stats = raw.agg(
            F.count("*").alias("raw_rows"),
            F.sum(F.when(
                F.col("goodreads_book_id").isNull() | F.col("tag_id").isNull()
                | F.col("count").isNull(), 1
            ).otherwise(0)).alias("null_rows"),
            F.sum(F.when(
                (F.col("goodreads_book_id") <= 0) | (F.col("tag_id") < 0), 1
            ).otherwise(0)).alias("invalid_id_rows"),
            F.sum(F.when(F.col("count") <= 0, 1).otherwise(0)).alias("nonpositive_rows"),
            F.countDistinct("goodreads_book_id", "tag_id").alias("raw_unique_pairs"),
        ).first().asDict()
        report.update(raw_stats)
        valid = keep(valid_book_tags(raw))
        report["valid_positive_rows"] = valid.count()
        deduped = keep(deduplicate_book_tags(valid))
        report["deduplicated_rows"] = deduped.count()
        report["duplicate_rows_removed"] = (
            report["valid_positive_rows"] - report["deduplicated_rows"]
        )
        report["duplicate_groups"] = valid.groupBy("goodreads_book_id", "tag_id").count().filter(
            F.col("count") > 1
        ).count()

        books = keep(spark.read.parquet(f"{hdfs_root}/processed/books").select(
            F.col("book_id").cast("int").alias("book_id"),
            F.col("goodreads_book_id").cast("long").alias("goodreads_book_id"),
        ))
        report["invalid_book_mapping_rows"] = books.filter(
            F.col("book_id").isNull() | F.col("goodreads_book_id").isNull()
            | (F.col("book_id") <= 0) | (F.col("goodreads_book_id") <= 0)
        ).count()
        if report["invalid_book_mapping_rows"]:
            raise ValueError(f"Invalid books ID mapping: {report}")
        require_unique(books, "book_id", "books mapping")
        require_unique(books, "goodreads_book_id", "books mapping")
        report["input_books"] = books.count()

        # String ingestion permits explicit detection of missing/invalid names.
        tag_schema = "tag_id INT, tag_name STRING"
        tags_raw = keep(spark.read.option("header", True).option("mode", "FAILFAST")
                        .schema(tag_schema).csv(f"{hdfs_root}/raw/tags.csv"))
        tags = keep(tags_raw.select("tag_id", normalize_tag_name(F.col("tag_name")).alias("tag_name")))
        report["invalid_tag_dictionary_rows"] = tags.filter(
            F.col("tag_id").isNull() | (F.col("tag_id") < 0)
            | F.col("tag_name").isNull() | (F.length("tag_name") == 0)
        ).count()
        if report["invalid_tag_dictionary_rows"]:
            raise ValueError(f"Invalid tag dictionary: {report}")
        tag_rows = tags.count()
        tags = keep(tags.distinct())
        report["tag_dictionary_duplicate_rows_removed"] = tag_rows - tags.count()
        require_unique(tags, "tag_id", "tag dictionary")

        report["unmatched_book_tag_rows"] = deduped.join(
            books, "goodreads_book_id", "left_anti"
        ).count()
        report["unmatched_tag_id_rows"] = deduped.join(tags, "tag_id", "left_anti").count()
        print("Input audit: " + json.dumps(report, ensure_ascii=False, sort_keys=True), flush=True)
        if report["unmatched_book_tag_rows"] or report["unmatched_tag_id_rows"]:
            raise ValueError("Unmatched IDs; refusing to silently discard shelf records")

        joined = keep(join_book_tags(deduped, books, tags))
        general = keep(normalize_weights(joined))
        topics = keep(normalize_weights(select_topics(joined)))
        report["general"] = quality_summary(general)
        report["topics"] = quality_summary(topics)
        report["books_without_general_tags"] = report["input_books"] - report["general"]["books"]
        report["books_without_topic_tags"] = report["input_books"] - report["topics"]["books"]
        report["non_allowlisted_rows"] = report["general"]["rows"] - report["topics"]["rows"]

        # Each run owns a new root; partial runs are never overwritten on retry.
        for name, frame in (("general", general), ("topics", topics)):
            path = f"{output_root}/{name}"
            frame.write.mode("errorifexists").parquet(path)
            persisted = quality_summary(spark.read.parquet(path))
            if persisted != report[name]:
                # Floating sums can differ slightly after repartitioning.
                for key in ("rows", "books", "unique_pairs", "invalid_rows"):
                    if persisted[key] != report[name][key]:
                        raise ValueError(f"Read-back mismatch for {path}: {persisted}")
            report[f"{name}_readback"] = persisted
        report_json = json.dumps(report, ensure_ascii=False, sort_keys=True)
        spark.createDataFrame([(report_json,)], ["value"]).coalesce(1).write.mode(
            "errorifexists"
        ).text(f"{output_root}/audit")
        print("Completed: " + report_json, flush=True)
        return report
    finally:
        for frame in reversed(cached):
            frame.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-root", default=HDFS_ROOT)
    parser.add_argument("--output-root", help="New, non-existing output directory")
    args = parser.parse_args()
    root = args.hdfs_root.rstrip("/")
    output = (args.output_root or f"{root}/processed/tag_features_v1").rstrip("/")
    spark = SparkSession.builder.appName("BookRec-TagPreparation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        prepare_tags(spark, root, output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
