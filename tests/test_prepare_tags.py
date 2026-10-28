"""Run with spark-submit --master local[2] tests/test_prepare_tags.py."""

import sys
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prepare_tags import (  # noqa: E402
    deduplicate_book_tags, ensure_new_output, join_book_tags,
    normalize_tag_name, normalize_weights, quality_summary,
    prepare_tags, require_unique, select_topics, valid_book_tags,
)


class TagFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("BookRec-TagTests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_positive_max_dedup_and_nonpositive_count_audit(self):
        raw = self.spark.createDataFrame([
            (100, 0, 6), (100, 0, 5), (100, 0, 6), (100, 1, 0),
            (100, 2, -1),
        ], "goodreads_book_id LONG, tag_id INT, count LONG")
        audit = {}
        actual = deduplicate_book_tags(valid_book_tags(raw, audit=audit)).collect()
        self.assertEqual([r.asDict() for r in actual], [
            {"goodreads_book_id": 100, "tag_id": 0, "tag_count": 6}
        ])
        self.assertEqual(audit, {"raw_rows": 5, "null_rows": 0, "invalid_id_rows": 0,
                                 "nonpositive_rows": 2, "raw_unique_pairs": 3})

    def test_null_fields_fail_instead_of_being_filtered(self):
        for row in ((None, 0, 2), (100, None, 2), (100, 0, None), (None, 0, 0)):
            with self.subTest(row=row):
                raw = self.spark.createDataFrame([(101, 1, 3), row],
                    "goodreads_book_id LONG, tag_id INT, count LONG")
                audit = {}
                with self.assertRaisesRegex(ValueError, "Invalid book_tags input"):
                    valid_book_tags(raw, audit=audit)
                self.assertEqual(audit["null_rows"], 1)

    def test_invalid_ids_fail_even_with_nonpositive_counts(self):
        for row in ((0, 0, 2), (-1, 0, 2), (100, -1, 2), (-1, 0, 0), (100, -1, -2)):
            with self.subTest(row=row):
                raw = self.spark.createDataFrame([(101, 1, 3), row],
                    "goodreads_book_id LONG, tag_id INT, count LONG")
                audit = {}
                with self.assertRaisesRegex(ValueError, "Invalid book_tags input"):
                    valid_book_tags(raw, audit=audit)
                self.assertEqual(audit["invalid_id_rows"], 1)

    def test_empty_input_is_rejected(self):
        raw = self.spark.createDataFrame([], "goodreads_book_id LONG, tag_id INT, count LONG")
        with self.assertRaisesRegex(ValueError, "Invalid book_tags input"):
            valid_book_tags(raw)

    def test_all_nonpositive_counts_remain_filterable(self):
        raw = self.spark.createDataFrame([(100, 0, 0), (101, 1, -1)],
            "goodreads_book_id LONG, tag_id INT, count LONG")
        audit = {}
        self.assertEqual(valid_book_tags(raw, audit=audit).count(), 0)
        self.assertEqual(audit["nonpositive_rows"], 2)
        self.assertEqual(audit["null_rows"], 0)
        self.assertEqual(audit["invalid_id_rows"], 0)

    def test_csv_anomalies_abort_before_mapping_reads_or_output(self):
        for row in (",0,2", "100,,0", "100,0,", "0,0,-1", "100,-1,2"):
            with self.subTest(csv=row), tempfile.TemporaryDirectory(prefix="bookrec-tag-input-") as directory:
                root = Path(directory)
                (root / "raw").mkdir()
                source = root / "raw/book_tags.csv"
                content = f"goodreads_book_id,tag_id,count\n{row}\n"
                source.write_text(content)
                output = root / "new-tags"
                # processed/books deliberately does not exist: the raw gate must fail first.
                with self.assertRaisesRegex(ValueError, "Invalid book_tags input"):
                    prepare_tags(self.spark, root.as_uri(), output.as_uri())
                self.assertFalse(output.exists())
                self.assertEqual(source.read_text(), content)

    def test_csv_type_errors_keep_failfast_behavior(self):
        for row in ("not-an-id,0,2", "100,2147483648,2", "100,0,not-a-count"):
            with self.subTest(csv=row), tempfile.TemporaryDirectory(prefix="bookrec-tag-input-") as directory:
                root = Path(directory)
                (root / "raw").mkdir()
                source = root / "raw/book_tags.csv"
                source.write_text(f"goodreads_book_id,tag_id,count\n{row}\n")
                output = root / "new-tags"
                with self.assertRaisesRegex(Exception, "MALFORMED_RECORD_IN_PARSING"):
                    prepare_tags(self.spark, root.as_uri(), output.as_uri())
                self.assertFalse(output.exists())

    def test_goodreads_join_and_separate_normalization(self):
        # The crossed IDs expose an accidental book_id = Goodreads ID join.
        books = self.spark.createDataFrame([(1, 100), (100, 1)],
                                           "book_id INT, goodreads_book_id LONG")
        tags = self.spark.createDataFrame([
            (0, " Fantasy "), (1, "to-read"), (2, "science_fiction"),
            (3, "currently-reading"), (4, "owned"), (5, "unreviewed-shelf"),
        ], "tag_id INT, tag_name STRING").select(
            "tag_id", normalize_tag_name(F.col("tag_name")).alias("tag_name")
        )
        bt = self.spark.createDataFrame([
            (100, 0, 3), (100, 1, 90), (100, 2, 1),
            (100, 3, 3), (100, 4, 2), (100, 5, 1), (1, 1, 5),
        ], "goodreads_book_id LONG, tag_id INT, tag_count LONG")
        joined = join_book_tags(bt, books, tags)
        general = normalize_weights(joined)
        topics = normalize_weights(select_topics(joined))
        self.assertEqual(quality_summary(general)["books"], 2)
        self.assertEqual(quality_summary(topics)["books"], 1)
        by_tag = {r.tag_name: r for r in topics.collect()}
        self.assertEqual(set(by_tag), {"fantasy", "science-fiction"})
        self.assertEqual(by_tag["fantasy"].book_id, 1)
        self.assertAlmostEqual(by_tag["fantasy"].tag_weight, 0.75)
        self.assertAlmostEqual(by_tag["science-fiction"].tag_weight, 0.25)
        self.assertAlmostEqual(general.filter("book_id = 1 AND tag_id = 0").first().tag_weight, 0.03)

    def test_duplicate_mapping_is_rejected(self):
        frame = self.spark.createDataFrame([(1, "a"), (1, "b")], "tag_id INT, tag_name STRING")
        with self.assertRaisesRegex(ValueError, "duplicate tag_id"):
            require_unique(frame, "tag_id", "test dictionary")

    def test_invalid_features_are_rejected(self):
        frame = self.spark.createDataFrame([(1, 0, "fantasy", 1, 0.5)],
            "book_id INT, tag_id INT, tag_name STRING, tag_count LONG, tag_weight DOUBLE")
        with self.assertRaisesRegex(ValueError, "Invalid tag features"):
            quality_summary(frame)

    def test_existing_output_is_rejected(self):
        with self.assertRaises(FileExistsError):
            ensure_new_output(self.spark, Path(__file__).resolve().parent.as_uri())


if __name__ == "__main__":
    unittest.main(verbosity=2)
