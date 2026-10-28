"""Pure configuration/identity tests; no data downloads, Spark or HDFS access."""

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datasets import DatasetSpec, GOODBOOKS_ROOT, GOODBOOKS_VERSION, require_artifact_binding
from prepare_dataset import load_spec


def fixture_config(**changes):
    config = {
        "dataset_id": "fixture-a", "data_version": "r1", "mapping_version": "m1", "model_version": "als1",
        "ratings_columns": {"user_id": "reader", "book_id": "isbn", "rating": "score"},
        "books_columns": {"book_id": "isbn", "title": "name", "authors": None},
        "rating_scale": {"source_min": 0, "source_max": 10, "target_min": 1,
                         "target_max": 5, "transform": "linear"}, "topics": "none",
    }
    config.update(changes)
    return config


class DatasetConfigTest(unittest.TestCase):
    def test_explicit_config_can_be_loaded_without_spark(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(fixture_config()))
            self.assertEqual(load_spec(path), DatasetSpec.from_dict(fixture_config()))

    def test_required_scale_and_unknown_fields_are_rejected(self):
        for field in ("rating_scale", "model_version", "topics"):
            config = fixture_config()
            config.pop(field)
            with self.subTest(field=field), self.assertRaises(ValueError):
                DatasetSpec.from_dict(config)
        with self.assertRaises(ValueError):
            DatasetSpec.from_dict(fixture_config(output_root=GOODBOOKS_ROOT))

    def test_unsafe_paths_and_reserved_goodbooks_names_are_rejected(self):
        for key in ("dataset_id", "data_version", "mapping_version", "model_version"):
            for value in ("", "../bookrec", "a/b", "hdfs://master:9000", "a%2fb"):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    DatasetSpec.from_dict(fixture_config(**{key: value}))
        for name in ("goodbooks10k", "goodbooks-10k", "GoodbooksCustom"):
            with self.assertRaises(ValueError):
                DatasetSpec.from_dict(fixture_config(dataset_id=name))

    def test_invalid_scales_and_implicit_conversion_are_rejected(self):
        for changes in ({"source_min": 10}, {"target_max": 1}, {"source_max": float("inf")},
                        {"source_max": True}, {"transform": "auto"}, {"transform": "preserve"},
                        {"source_min": -1e308, "source_max": 1e308}):
            config = fixture_config()
            config["rating_scale"].update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                DatasetSpec.from_dict(config)

    def test_field_mapping_must_be_complete_and_unambiguous(self):
        for fields in ({"user_id": "u"}, {"user_id": "x", "book_id": "x", "rating": "r"},
                       {"user_id": "u.id", "book_id": "b", "rating": "r"}):
            with self.assertRaises(ValueError):
                DatasetSpec.from_dict(fixture_config(ratings_columns=fields))

    def test_dataset_and_release_paths_are_separate(self):
        a = DatasetSpec.from_dict(fixture_config())
        b = DatasetSpec.from_dict(fixture_config(dataset_id="fixture-b"))
        c = DatasetSpec.from_dict(fixture_config(data_version="r2"))
        self.assertNotEqual(a.raw_root, b.raw_root)
        self.assertNotEqual(a.prepared_root, b.prepared_root)
        self.assertNotEqual(a.model_path, b.model_path)
        self.assertNotEqual(a.prepared_root, c.prepared_root)
        self.assertTrue(a.prepared_root.startswith(GOODBOOKS_ROOT + "/datasets/"))
        self.assertNotEqual(a.model_path, GOODBOOKS_ROOT + "/models/als_full_v1")

    def test_cache_identity_covers_dataset_schema_mapping_scale_and_model(self):
        original = DatasetSpec.from_dict(fixture_config())
        for changes in ({"dataset_id": "fixture-b"}, {"data_version": "r2"},
                        {"mapping_version": "m2"}, {"model_version": "als2"},
                        {"ratings_columns": {"user_id": "u", "book_id": "b", "rating": "r"}}):
            spec = DatasetSpec.from_dict(fixture_config(**changes))
            self.assertNotEqual(spec.cache_version, original.cache_version)
        config = fixture_config()
        config["rating_scale"].update(target_min=0, target_max=10, transform="preserve")
        self.assertNotEqual(DatasetSpec.from_dict(config).cache_version, original.cache_version)
        self.assertNotEqual(original.cache_version, GOODBOOKS_VERSION)

    def test_model_version_does_not_relocate_or_reidentify_prepared_data(self):
        a = DatasetSpec.from_dict(fixture_config())
        b = DatasetSpec.from_dict(fixture_config(model_version="als2"))
        self.assertEqual(a.prepared_root, b.prepared_root)
        self.assertEqual(a.data_binding, b.data_binding)
        self.assertNotEqual(a.model_path, b.model_path)
        self.assertNotEqual(a.model_binding, b.model_binding)

    def test_manifests_reject_cross_dataset_and_model_reuse(self):
        a = DatasetSpec.from_dict(fixture_config())
        for kind, binding in (("prepared", a.data_binding), ("als", a.model_binding)):
            manifest = {"artifact_kind": kind, "binding": binding}
            require_artifact_binding(manifest, a, kind)
            for changes in ({"dataset_id": "fixture-b"}, {"data_version": "r2"}, {"mapping_version": "m2"}):
                with self.assertRaises(ValueError):
                    require_artifact_binding(manifest, DatasetSpec.from_dict(fixture_config(**changes)), kind)
        with self.assertRaises(ValueError):
            require_artifact_binding({"artifact_kind": "als", "binding": a.model_binding},
                                     DatasetSpec.from_dict(fixture_config(model_version="als2")), "als")

    def test_input_dictionary_changes_do_not_mutate_spec(self):
        config = fixture_config()
        spec = DatasetSpec.from_dict(config)
        original = copy.deepcopy(spec.data_binding)
        config["ratings_columns"]["user_id"] = "different"
        self.assertEqual(spec.data_binding, original)

    def test_unsupported_topic_format_requires_a_real_adapter(self):
        with self.assertRaisesRegex(ValueError, "legacy adapter"):
            DatasetSpec.from_dict(fixture_config(topics="guess-goodreads"))

    def test_cli_help_does_not_require_pyspark(self):
        script = Path(__file__).resolve().parents[1] / "src/prepare_dataset.py"
        result = subprocess.run([sys.executable, str(script), "--help"],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--previous-config", result.stdout)
        self.assertNotIn("--overwrite", result.stdout)

    def test_framework_is_also_importable_as_package_without_spark(self):
        result = subprocess.run([sys.executable, "-c",
            "from src.datasets import DatasetSpec; from src.dataset_adapter import adapt_frames; "
            "from src.prepare_dataset import prepare"],
            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
