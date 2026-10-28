"""spark-submit --master local[2]; artificial data and file:///tmp only, no ALS fit."""

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dataset_adapter import IdMappings, adapt_frames
from datasets import DatasetSpec, require_artifact_binding
from prepare_dataset import prepare
from test_datasets import fixture_config


def digests(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file()}


class DatasetAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("BookRec-DatasetAdapterTests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spec = DatasetSpec.from_dict(fixture_config())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self, rows=None, metadata=None):
        ratings = self.spark.createDataFrame(rows if rows is not None else [
            ("u2", "001", "0"), ("u1", "book-x", "10"), ("u2", "book-x", "2.5")],
            "reader STRING, isbn STRING, score STRING")
        books = self.spark.createDataFrame(metadata if metadata is not None else [
            ("001", "First"), ("book-x", "Second"), ("1", "Unrated")], "isbn STRING, name STRING")
        return ratings, books

    def test_canonical_fields_explicit_scale_and_optional_topics(self):
        result = adapt_frames(*self.frames(), self.spec, mapping_mode="create")
        self.assertEqual(result.ratings.schema.simpleString(), "struct<user_id:int,book_id:int,rating:double>")
        self.assertEqual(result.books.schema.simpleString(), "struct<book_id:int,title:string,authors:string>")
        self.assertEqual(sorted(r.rating for r in result.ratings.collect()), [1.0, 2.0, 5.0])
        self.assertTrue(all(row.authors is None for row in result.books.collect()))
        self.assertEqual(result.summary(), {"ratings": 3, "users": 2, "min_rating": 1.0,
            "max_rating": 5.0, "books": 3, "topics_available": False,
            "topic_message": "该数据集未提供主题标签，主题画像暂不可用。"})
        preview = result.preview_popularity(1)
        self.assertEqual(preview["algorithm"], "observed_popularity_preview")
        self.assertEqual([row["title"] for row in preview["recommendations"]], ["First", "Unrated"])
        self.assertFalse(preview["topics_available"])

    def test_arbitrary_field_names_and_preserve_scale(self):
        config = fixture_config(ratings_columns={"user_id": "who", "book_id": "item", "rating": "value"},
                                books_columns={"book_id": "item", "title": "caption", "authors": "writer"})
        config["rating_scale"].update(target_min=0, target_max=10, transform="preserve")
        r = self.spark.createDataFrame([("x", "b", "8.5")], "who STRING, item STRING, value STRING")
        b = self.spark.createDataFrame([("b", "Title", "Author")], "item STRING, caption STRING, writer STRING")
        result = adapt_frames(r, b, DatasetSpec.from_dict(config), mapping_mode="create")
        self.assertEqual(result.ratings.first().rating, 8.5)
        self.assertEqual(result.books.first().authors, "Author")

    def test_mapping_is_stable_under_input_reordering_and_preserves_source_strings(self):
        r, b = self.frames()
        first = adapt_frames(r, b, self.spec, mapping_mode="create")
        second = adapt_frames(r.orderBy(r.reader.desc()), b.orderBy(b.isbn.desc()), self.spec, mapping_mode="create")
        for left, right in ((first.mappings.users, second.mappings.users), (first.mappings.books, second.mappings.books)):
            self.assertEqual(left.orderBy("source_id").collect(), right.orderBy("source_id").collect())
        mapping = {row.source_id: row.book_id for row in first.mappings.books.collect()}
        self.assertNotEqual(mapping["001"], mapping["1"])
        self.assertIn("book-x", mapping)

    def test_mapping_extensions_preserve_persisted_ids_and_reject_other_datasets(self):
        users = self.spark.createDataFrame([("u1", 8), ("u2", 9)], "source_id STRING, user_id INT")
        books = self.spark.createDataFrame([("book-x", 12)], "source_id STRING, book_id INT")
        previous = IdMappings(self.spec.dataset_id, self.spec.mapping_version, users, books)
        r, b = self.frames(rows=[("a-new", "001", "4"), ("u1", "book-x", "6")])
        result = adapt_frames(r, b, self.spec, previous, mapping_mode="extend")
        self.assertEqual({row.source_id: row.user_id for row in result.mappings.users.collect()},
                         {"u1": 8, "u2": 9, "a-new": 10})
        self.assertEqual({row.source_id: row.book_id for row in result.mappings.books.collect()}["book-x"], 12)
        with self.assertRaisesRegex(ValueError, "across datasets"):
            adapt_frames(r, b, self.spec, IdMappings("fixture-b", "m1", users, books), mapping_mode="extend")
        bad = self.spark.createDataFrame([("u1", 1), ("u2", 1)], "source_id STRING, user_id INT")
        with self.assertRaisesRegex(ValueError, "Duplicate internal"):
            adapt_frames(r, b, self.spec, IdMappings(self.spec.dataset_id, "m1", bad, books), mapping_mode="extend")

    def test_duplicate_rating_keys_and_book_ids_are_rejected(self):
        for r, b in (self.frames(rows=[("u", "001", "2"), ("u", "001", "4")]),
                     self.frames(metadata=[("001", "A"), ("001", "B"), ("book-x", "C")])):
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                adapt_frames(r, b, self.spec, mapping_mode="create")

    def test_missing_ids_metadata_or_columns_are_rejected(self):
        for row in ((None, "001", "4"), ("u", None, "4"), (" ", "001", "4"), ("u", "absent", "4")):
            with self.subTest(row=row), self.assertRaises(ValueError):
                adapt_frames(*self.frames(rows=[row]), self.spec, mapping_mode="create")
        for metadata in ([(None, "A"), ("book-x", "B")], [("001", None), ("book-x", "B")]):
            with self.assertRaises(ValueError):
                adapt_frames(*self.frames(metadata=metadata), self.spec, mapping_mode="create")
        r, b = self.frames()
        with self.assertRaisesRegex(ValueError, "Missing declared"):
            adapt_frames(r.drop("reader"), b, self.spec, mapping_mode="create")

    def test_invalid_ratings_are_never_clipped_or_dropped(self):
        for rating in (None, "-1", "11", "NaN", "Infinity", "not-a-rating"):
            with self.subTest(rating=rating), self.assertRaisesRegex(ValueError, "Invalid explicit rating"):
                adapt_frames(*self.frames(rows=[("u", "001", rating)]), self.spec, mapping_mode="create")

    def test_prepare_writes_only_new_local_release_and_refuses_retry(self):
        with tempfile.TemporaryDirectory(prefix="bookrec-dataset-") as directory:
            root = Path(directory)
            raw = root / "raw/r1"
            raw.mkdir(parents=True)
            (raw / "ratings.csv").write_text("reader,isbn,score\nu,001,8\n")
            (raw / "books.csv").write_text("isbn,name\n001,Example\n")
            original = digests(raw)
            target = root / "releases/r1/m1"
            with patch.object(DatasetSpec, "root", property(lambda spec: root.as_uri())):
                manifest = prepare(self.spark, self.spec, mapping_mode="create")
                require_artifact_binding(manifest, self.spec)
                saved = json.loads(self.spark.read.text(f"{target.as_uri()}/manifest").first().value)
                self.assertEqual(saved, manifest)
                self.assertEqual(self.spark.read.parquet(f"{target.as_uri()}/processed/ratings").first().rating, 4.2)
                self.assertEqual(self.spark.read.parquet(f"{target.as_uri()}/mappings/books").first().source_id, "001")
                before = digests(target)
                with self.assertRaises(FileExistsError):
                    prepare(self.spark, self.spec, mapping_mode="create")
                self.assertEqual(digests(target), before)
            self.assertEqual(digests(raw), original)

    def test_new_data_version_must_inherit_existing_mapping_version(self):
        """Omitting previous-config must not reset IDs, even via explicit create."""
        with tempfile.TemporaryDirectory(prefix="bookrec-lineage-") as directory:
            root = Path(directory)
            old = self.spec
            new = DatasetSpec.from_dict(fixture_config(data_version="r2"))
            later = DatasetSpec.from_dict(fixture_config(data_version="r3"))
            with patch.object(DatasetSpec, "root", property(lambda spec: root.as_uri())):
                # Synthetic pre-fix release: no mapping_parent field, valid prior bindings.
                prior = root / "releases/r1/m1"
                manifest = {"artifact_kind": "prepared", "binding": old.data_binding}
                self.spark.createDataFrame([(json.dumps(manifest),)], ["value"]).write.text(
                    f"{prior.as_uri()}/manifest")
                self.spark.createDataFrame([("u-old", 8), ("absent-user", 9)],
                    "source_id STRING, user_id INT").write.parquet(f"{prior.as_uri()}/mappings/users")
                self.spark.createDataFrame([("z-old", 12), ("absent-book", 13)],
                    "source_id STRING, book_id INT").write.parquet(f"{prior.as_uri()}/mappings/books")
                before = digests(prior)
                # No r2 raw data yet: all three failures must precede any raw read/write.
                with self.assertRaisesRegex(ValueError, "previous-config"):
                    prepare(self.spark, new, mapping_mode="extend")
                with self.assertRaisesRegex(ValueError, "already exists"):
                    prepare(self.spark, new, mapping_mode="create")
                with self.assertRaisesRegex(ValueError, "mapping-mode"):
                    prepare(self.spark, new)
                self.assertFalse((root / "releases/r2").exists())
                raw = root / "raw/r2"
                raw.mkdir(parents=True)
                (raw / "ratings.csv").write_text("reader,isbn,score\na-new,a-new,6\nu-old,z-old,8\n")
                (raw / "books.csv").write_text("isbn,name\na-new,New\nz-old,Old\n")
                report = prepare(self.spark, new, old, mapping_mode="extend")
                self.assertEqual(report["mapping_parent"], old.data_binding)
                users = self.spark.read.parquet(f"{new.prepared_root}/mappings/users")
                books = self.spark.read.parquet(f"{new.prepared_root}/mappings/books")
                self.assertEqual({r.source_id: r.user_id for r in users.collect()},
                                 {"u-old": 8, "absent-user": 9, "a-new": 10})
                self.assertEqual({r.source_id: r.book_id for r in books.collect()},
                                 {"z-old": 12, "absent-book": 13, "a-new": 14})
                # Inheriting r1 again would omit the IDs added by r2.
                with self.assertRaisesRegex(ValueError, "latest mapping release"):
                    prepare(self.spark, later, old, mapping_mode="extend")
                self.assertFalse((root / "releases/r3").exists())
                self.assertEqual(digests(prior), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
