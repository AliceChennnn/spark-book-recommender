"""Run with spark-submit --master local[2] tests/test_recommenders.py."""

import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recommend import build_parser, display_result, recommend  # noqa: E402
from recommenders import (  # noqa: E402
    BookRecommender, RECOMMENDATION_FIELDS, final_test_condition, training_popularity,
)


class FixedALSModel:
    """Fixed predictions test inference without fitting or saving any model."""

    def __init__(self, spark, users=(20,)):
        self.userFactors = spark.createDataFrame([(u,) for u in users], "id INT")
        self.scores = spark.createDataFrame([
            (1, 9.0), (2, 4.5), (3, 4.5), (4, 3.0), (5, float("nan")),
        ], "book_id INT, prediction DOUBLE")

    def setColdStartStrategy(self, strategy):
        return self

    def transform(self, frame):
        return frame.join(self.scores, "book_id", "left")


class RecommendersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("BookRec-RecommenderTests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
        cls.ratings = cls.spark.createDataFrame([
            (20, 1, 5), (40, 1, 4), (60, 2, 3), (80, 2, 5),
            (100, 3, 4), (120, 4, 5), (140, 4, 4),
        ], "user_id INT, book_id INT, rating INT")
        cls.books = cls.spark.createDataFrame(
            [(b, f"Book {b}", "A, B") for b in range(1, 7)],
            "book_id INT, title STRING, authors STRING",
        )
        cls.model = FixedALSModel(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.service = BookRecommender(self.spark, self.ratings, self.books, self.model)

    def tearDown(self):
        self.service.close()

    def test_als_order_top_k_and_structure(self):
        result = self.service.recommend(20, 2)
        rows = result["recommendations"]
        self.assertEqual([r["book_id"] for r in rows], [2, 3])
        self.assertEqual([r["rank"] for r in rows], [1, 2])
        self.assertEqual(result["actual_algorithm"], "als")
        for row in rows:
            self.assertEqual(set(row), set(RECOMMENDATION_FIELDS))
            self.assertEqual(row["algorithm"], "als")
            self.assertEqual(row["score_type"], "als_predicted_rating")
            self.assertEqual(row["score"], 4.5)
            self.assertEqual(row["user_id"], 20)

    def test_popularity_counts_order_and_seen_exclusion(self):
        with patch.object(self.service, "_get_model", side_effect=AssertionError("unexpected model load")):
            result = self.service.recommend(20, 4, "popularity")
        rows = result["recommendations"]
        self.assertEqual([r["book_id"] for r in rows], [2, 4, 3, 5])
        self.assertEqual([r["score"] for r in rows], [2, 2, 1, 0])
        self.assertNotIn(1, [r["book_id"] for r in rows])
        self.assertTrue(all(r["algorithm"] == "popularity" for r in rows))
        self.assertTrue(all(r["score_type"] == "training_rating_count" for r in rows))

    def test_unknown_user_fallback_matches_explicit_popularity(self):
        fallback = self.service.recommend(999999, 3)
        popular = self.service.recommend(999999, 3, "popularity")
        self.assertEqual(fallback["requested_algorithm"], "als")
        self.assertEqual(fallback["actual_algorithm"], "popularity")
        self.assertEqual(fallback["fallback_reason"], "unknown_als_user")
        self.assertTrue(fallback["messages"])
        self.assertEqual(fallback["recommendations"], popular["recommendations"])
        self.assertEqual([r["book_id"] for r in fallback["recommendations"]], [1, 2, 4])

    def test_unknown_items_and_nonfinite_scores_are_skipped(self):
        result = self.service.recommend(20, 10)
        self.assertEqual(result["status"], "partial")
        self.assertEqual([r["book_id"] for r in result["recommendations"]], [2, 3, 4])
        self.assertEqual(result["actual_algorithm"], "als")
        with patch.object(self.model, "transform", side_effect=lambda frame: frame.withColumn(
                "prediction", F.lit(float("inf")))):
            self.assertEqual(self.service.recommend(20)["recommendations"], [])

    def test_empty_candidates_both_modes(self):
        ratings = self.spark.createDataFrame([(20, b, 5) for b in range(1, 7)], self.ratings.schema)
        service = BookRecommender(self.spark, ratings, self.books, self.model)
        try:
            for mode in ("als", "popularity"):
                result = service.recommend(20, 10, mode)
                self.assertEqual(result["recommendations"], [])
                self.assertEqual(result["status"], "empty")
                self.assertEqual(result["actual_algorithm"], mode)
                self.assertTrue(result["messages"])
        finally:
            service.close()

    def test_holdout_exclusion_matches_original_hash_rule(self):
        options = self.spark.range(1, 200).select(
            F.col("id").cast("int").alias("user_id"), F.lit(2).alias("book_id"), F.lit(5).alias("rating")
        )
        original = "pmod(user_id, 20) != 0 AND pmod(xxhash64(user_id, book_id), 10) = 0"
        heldout = options.filter(original).first()
        self.assertIsNotNone(heldout)
        ratings = self.ratings.union(self.spark.createDataFrame([heldout], self.ratings.schema))
        self.assertEqual(ratings.filter(final_test_condition()).count(), 1)
        counts = {r.book_id: r.popularity for r in training_popularity(ratings).collect()}
        self.assertEqual(counts, {1: 2, 2: 2, 3: 1, 4: 2})
        expected = {r.book_id: r["count"] for r in ratings.filter(f"NOT ({original})").groupBy("book_id").count().collect()}
        self.assertEqual(counts, expected)
        service = BookRecommender(self.spark, ratings, self.books, FixedALSModel(self.spark, (heldout.user_id,)))
        try:
            for mode in ("als", "popularity"):
                self.assertNotIn(2, [r["book_id"] for r in service.recommend(heldout.user_id, 10, mode)["recommendations"]])
        finally:
            service.close()

    def test_hash_input_schema_cannot_silently_change(self):
        with self.assertRaisesRegex(TypeError, "INT schema"):
            training_popularity(self.ratings.withColumn("book_id", F.col("book_id").cast("long")))

    def test_popularity_cache_is_reused(self):
        with patch("recommenders.training_popularity", wraps=training_popularity) as build:
            self.service.recommend(20, 1, "popularity")
            self.service.recommend(999999, 1, "popularity")
            self.assertEqual(build.call_count, 1)

    def test_invalid_parameters(self):
        for args in ((0, 10, "als"), (-1, 10, "als"), (True, 10, "als"),
                     (2147483648, 10, "als"), (20, 0, "als"), (20, -1, "als"),
                     (20, 1.5, "als"), (20, 1, "other")):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.service.recommend(*args)

    def test_old_cli_defaults_and_three_argument_function(self):
        args = build_parser().parse_args(["--user-id", "20", "--top-k", "2"])
        self.assertEqual(args.mode, "als")
        self.assertEqual(args.format, "table")
        output = io.StringIO()
        with patch("recommend.BookRecommender.from_hdfs", return_value=self.service), contextlib.redirect_stdout(output):
            result = recommend(self.spark, 20, 2)
        self.assertIn("predicted_rating", output.getvalue())
        self.assertIn("Recommendations for User 20", output.getvalue())
        self.assertEqual([r["book_id"] for r in result["recommendations"]], [2, 3])

    def test_cli_fallback_notice_and_json(self):
        result = self.service.recommend(999999, 1)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            display_result(result)
        self.assertIn("请求算法: als", output.getvalue())
        self.assertIn("实际使用算法: popularity", output.getvalue())
        self.assertIn("回退", output.getvalue())
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            display_result(result, "json")
        self.assertEqual(json.loads(output.getvalue()), result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
