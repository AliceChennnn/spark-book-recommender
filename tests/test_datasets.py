"""Pure configuration/identity tests; no data downloads, Spark or HDFS access."""

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datasets import DatasetSpec, GOODBOOKS_ROOT, GOODBOOKS_VERSION, require_artifact_binding
from dataset_adapter import adapt_frames
from prepare_dataset import load_spec, prepare, _check_mapping_history


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

    def test_instance_field_mappings_cannot_be_mutated(self):
        spec = DatasetSpec.from_dict(fixture_config())
        before = (spec.preparation_config, spec.data_binding, spec.model_binding, spec.cache_version)
        for fields in (spec.ratings_columns, spec.books_columns):
            with self.assertRaises(TypeError):
                fields["book_id"] = "changed"
            with self.assertRaises(TypeError):
                del fields["book_id"]
            self.assertFalse(hasattr(fields, "update"))
            self.assertFalse(hasattr(fields, "clear"))
        with self.assertRaises(FrozenInstanceError):
            spec.ratings_columns = {}
        self.assertEqual((spec.preparation_config, spec.data_binding, spec.model_binding, spec.cache_version), before)

    def test_export_is_detached_and_binding_remains_backward_compatible(self):
        spec = DatasetSpec.from_dict(fixture_config())
        exported = spec.preparation_config
        json.dumps(exported)
        exported["ratings_columns"]["user_id"] = "changed"
        exported["books_columns"]["authors"] = "changed"
        exported["rating_scale"]["source_max"] = 100
        self.assertEqual(spec.ratings_columns["user_id"], "reader")
        self.assertIsNone(spec.books_columns["authors"])
        self.assertEqual(spec.data_binding["preparation_sha256"],
                         "619fddf5d6a9659e3788847096e3edad80b083012f24e36da3780d3e9d739844")
        self.assertEqual(spec.cache_version,
                         "dataset-fixture-a-ae71df13afb38ba9e9cec173ed9e4c29311b421e2181a069ca8f0613207a180a")
        self.assertEqual(replace(spec, model_version="als2").data_binding, spec.data_binding)

    def test_mapping_modes_fail_before_any_spark_access(self):
        spec = DatasetSpec.from_dict(fixture_config(data_version="r2"))
        for mode, previous in ((None, None), ("extend", None), ("create", spec)):
            spark = Mock()
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                prepare(spark, spec, previous, mapping_mode=mode)
            self.assertEqual(spark.mock_calls, [])
            with self.assertRaises(ValueError):
                adapt_frames(None, None, spec, previous, mapping_mode=mode)

    def test_cli_extend_without_previous_config_fails_before_loading_config_or_spark(self):
        for args in (("--config", "/absent.json", "--mapping-mode", "extend"),
                     ("--config", "/absent.json")):
            script = Path(__file__).resolve().parents[1] / "src/prepare_dataset.py"
            result = subprocess.run([sys.executable, str(script), *args],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2)
            self.assertNotIn("Traceback", result.stderr)
            self.assertIn("--previous-config" if args[-1] == "extend" else "--mapping-mode", result.stderr)

    def test_create_rejects_mapping_version_used_by_another_data_version(self):
        spec = DatasetSpec.from_dict(fixture_config(data_version="r2"))
        spark = Mock()
        fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
        fs.globStatus.return_value = [Mock()]
        with self.assertRaisesRegex(ValueError, "already exists"):
            _check_mapping_history(spark, spec, None, "create")
        spark.read.text.assert_not_called()
        spark.read.option.assert_not_called()

    def history_spark(self, records):
        spark = Mock()
        statuses = []
        documents = {}
        for spec, parent in records:
            status = Mock()
            status.isDirectory.return_value = True
            status.getPath.return_value.toString.return_value = spec.prepared_root
            status.getPath.return_value.getParent.return_value.getName.return_value = spec.data_version
            statuses.append(status)
            manifest = {"artifact_kind": "prepared", "binding": spec.data_binding,
                        "mapping_parent": parent}
            documents[f"{spec.prepared_root}/manifest"] = manifest
        fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
        fs.globStatus.return_value = statuses
        def read(path):
            return Mock(collect=Mock(return_value=[Mock(value=json.dumps(documents[path]))]))
        spark.read.text.side_effect = read
        return spark

    def test_extend_requires_the_head_instead_of_lexical_data_version_order(self):
        a = DatasetSpec.from_dict(fixture_config(data_version="r2"))
        b = DatasetSpec.from_dict(fixture_config(data_version="r10"))
        target = DatasetSpec.from_dict(fixture_config(data_version="next"))
        spark = self.history_spark([(a, None), (b, a.data_binding)])
        _check_mapping_history(spark, target, b, "extend")
        with self.assertRaisesRegex(ValueError, "latest mapping release"):
            _check_mapping_history(spark, target, a, "extend")

    def test_independent_or_disconnected_histories_are_not_silently_merged(self):
        a = DatasetSpec.from_dict(fixture_config())
        b = DatasetSpec.from_dict(fixture_config(data_version="r2"))
        c = DatasetSpec.from_dict(fixture_config(data_version="r3"))
        target = DatasetSpec.from_dict(fixture_config(data_version="r4"))
        for records in ([(a, None), (b, None)],
                        [(a, None), (b, c.data_binding), (c, b.data_binding)],
                        [(a, b.data_binding)]):
            with self.subTest(records=len(records)), self.assertRaises(ValueError):
                _check_mapping_history(self.history_spark(records), target, a, "extend")

    def test_incomplete_release_and_filesystem_errors_fail_closed(self):
        old = DatasetSpec.from_dict(fixture_config())
        target = replace(old, data_version="r2")
        spark = self.history_spark([(old, None)])
        spark.read.text.side_effect = FileNotFoundError("partial release")
        with self.assertRaisesRegex(ValueError, "Incomplete or unreadable"):
            _check_mapping_history(spark, target, old, "extend")
        spark.read.option.assert_not_called()
        fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
        fs.globStatus.side_effect = OSError("filesystem unavailable")
        with self.assertRaisesRegex(OSError, "filesystem unavailable"):
            _check_mapping_history(spark, target, None, "create")

    def test_mapping_version_change_is_an_explicit_new_dictionary(self):
        old = DatasetSpec.from_dict(fixture_config())
        target = replace(old, data_version="r2", mapping_version="m2")
        spark = Mock()
        fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
        fs.globStatus.return_value = None
        _check_mapping_history(spark, target, None, "create")
        spark._jvm.org.apache.hadoop.fs.Path.assert_called_with(f"{target.root}/releases/*/m2")
        with self.assertRaisesRegex(ValueError, "same dataset and mapping version"):
            _check_mapping_history(spark, target, old, "extend")

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
