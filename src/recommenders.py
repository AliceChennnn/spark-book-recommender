"""Reusable ALS and training-count popularity recommendations."""

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


HDFS_ROOT = "hdfs://master:9000/user/root/bookrec"
MODEL_PATH = f"{HDFS_ROOT}/models/als_full_v1"
RECOMMENDATION_FIELDS = (
    "user_id", "book_id", "title", "authors", "rank", "score", "score_type", "algorithm",
)
SCORE_TYPES = {"als": "als_predicted_rating", "popularity": "training_rating_count"}


def final_test_condition():
    """Match train_final.py exactly, including column order and integer types."""
    return (
        (F.pmod(F.col("user_id"), F.lit(20)) != 0)
        & (F.pmod(F.xxhash64("user_id", "book_id"), F.lit(10)) == 0)
    )


def training_popularity(ratings):
    # xxhash64 differs for int and long inputs; never silently cast these IDs.
    for name in ("user_id", "book_id"):
        if ratings.schema[name].dataType != IntegerType():
            raise TypeError(f"{name} must retain the INT schema used by train_final.py")
    return ratings.filter(~final_test_condition()).groupBy("book_id").agg(
        F.count("*").alias("popularity")
    )


def validate_request(user_id, top_k, mode):
    if type(user_id) is not int or not 1 <= user_id <= 2147483647:
        raise ValueError("user_id must be a positive 32-bit integer")
    if type(top_k) is not int or top_k < 1:
        raise ValueError("top_k must be a positive integer")
    if mode not in SCORE_TYPES:
        raise ValueError("mode must be als or popularity")


class BookRecommender:
    """Reuse one loaded model and one small cached popularity table per instance."""

    def __init__(self, spark, ratings, books, model=None, model_path=MODEL_PATH):
        self.spark = spark
        self.ratings = ratings.select("user_id", "book_id", "rating")
        self.books = books.select("book_id", "title", "authors")
        self.model_path = model_path
        self._model = model
        self._popularity = None
        self._known_users = {}

    @classmethod
    def from_hdfs(cls, spark, hdfs_root=HDFS_ROOT):
        root = hdfs_root.rstrip("/")
        return cls(
            spark, spark.read.parquet(f"{root}/processed/ratings"),
            spark.read.parquet(f"{root}/processed/books"),
            model_path=f"{root}/models/als_full_v1",
        )

    def _get_model(self):
        if self._model is None:
            self._model = ALSModel.load(self.model_path)
        self._model.setColdStartStrategy("drop")
        return self._model

    def _get_popularity(self):
        if self._popularity is None:
            # About 10k counts, rather than persisting millions of rating rows.
            self._popularity = training_popularity(self.ratings).coalesce(4).cache()
        return self._popularity

    def close(self):
        """Release owned cache without stopping the caller's SparkSession."""
        if self._popularity is not None:
            self._popularity.unpersist()
            self._popularity = None

    def recommend(self, user_id, top_k=10, mode="als"):
        validate_request(user_id, top_k, mode)
        actual = mode
        fallback_reason = None
        messages = []
        model = None
        if mode == "als":
            model = self._get_model()
            if user_id not in self._known_users:
                self._known_users[user_id] = bool(
                    model.userFactors.filter(F.col("id") == user_id).limit(1).count()
                )
            if not self._known_users[user_id]:
                actual = "popularity"
                fallback_reason = "unknown_als_user"
                messages.append(f"用户 {user_id} 不在 ALS 模型中，已回退到 Popularity。")

        # Display recommendations exclude ALL observed ratings, including holdout.
        seen = self.ratings.filter(F.col("user_id") == user_id).select("book_id").distinct()
        candidates = self.books.join(F.broadcast(seen), "book_id", "left_anti").withColumn(
            "user_id", F.lit(user_id).cast("int")
        )
        if actual == "als":
            scored = model.transform(candidates).withColumn("score", F.col("prediction").cast("double"))
            scored = scored.filter(
                F.col("score").isNotNull() & ~F.isnan("score")
                & (F.abs(F.col("score")) != F.lit(float("inf")))
            )
        else:
            scored = candidates.join(F.broadcast(self._get_popularity()), "book_id", "left").withColumn(
                "score", F.coalesce(F.col("popularity"), F.lit(0)).cast("double")
            )
        rows = scored.orderBy(F.desc("score"), F.asc("book_id")).limit(top_k).select(
            "book_id", "title", "authors", "score"
        ).collect()
        recommendations = [
            {"user_id": user_id, **row.asDict(), "rank": rank,
             "score_type": SCORE_TYPES[actual], "algorithm": actual}
            for rank, row in enumerate(rows, 1)
        ]
        status = "ok" if len(rows) == top_k else ("partial" if rows else "empty")
        if status != "ok":
            messages.append("可评分的未评分候选图书不足，返回所有可用结果。" if rows
                            else "没有可评分的未评分候选图书。")
        return {"user_id": user_id, "requested_algorithm": mode, "actual_algorithm": actual,
                "fallback_reason": fallback_reason, "status": status,
                "top_k": top_k, "returned_count": len(rows),
                "messages": messages, "recommendations": recommendations}
