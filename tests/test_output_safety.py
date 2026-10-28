"""Host-only safety tests: no Spark session, Docker, training or real output writes."""

import contextlib
import importlib
import io
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from output_safety import ensure_outputs_available, save_model, write_parquet  # noqa: E402

MODULES = ("train_final", "train_als", "prepare_data")


def fake_spark(existing=()):
    spark = Mock()
    path_factory = spark._jvm.org.apache.hadoop.fs.Path
    fs = Mock()

    def make_path(destination):
        path = Mock()
        path.destination = destination
        path.getFileSystem.return_value = fs
        return path

    path_factory.side_effect = make_path
    fs.exists.side_effect = lambda path: path.destination in existing
    return spark, fs


def fake_pyspark(spark):
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = Mock()
    sql.SparkSession.builder.appName.return_value.getOrCreate.return_value = spark
    return {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": sql}


class OutputSafetyTest(unittest.TestCase):
    def test_imports_never_import_pyspark(self):
        code = """
import builtins, importlib, sys
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'pyspark' or name.startswith('pyspark.'):
        raise AssertionError('Import must not start Spark')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
sys.path.insert(0, sys.argv[1])
for name in ('train_final', 'train_als', 'prepare_data'):
    importlib.import_module(name)
"""
        result = subprocess.run([sys.executable, "-c", code, str(SRC)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_legacy_arguments_and_explicit_flag(self):
        for name in MODULES:
            with self.subTest(script=name):
                parser = importlib.import_module(name).build_parser()
                self.assertFalse(parser.parse_args([]).overwrite)
                self.assertTrue(parser.parse_args(["--overwrite"]).overwrite)

    def test_help_and_invalid_flags_do_not_start_spark(self):
        for name in MODULES:
            module = importlib.import_module(name)
            for argv, code in ((["--help"], 0), (["--over"], 2),
                               (["--overwrite=false"], 2), (["--overwrite", "false"], 2)):
                with self.subTest(script=name, argv=argv):
                    spark, _ = fake_spark()
                    packages = fake_pyspark(spark)
                    with patch.dict(sys.modules, packages), contextlib.redirect_stdout(io.StringIO()), \
                            contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                        module.main(argv)
                    self.assertEqual(error.exception.code, code)
                    packages["pyspark.sql"].SparkSession.builder.appName.assert_not_called()

    def test_preflight_resolves_each_paths_own_filesystem(self):
        spark, fs = fake_spark()
        paths = ["hdfs://master:9000/new-model", "file:///tmp/new-parquet"]
        ensure_outputs_available(spark, paths)
        self.assertEqual(fs.exists.call_count, 2)
        for call in fs.exists.call_args_list:
            call.args[0].getFileSystem.assert_called_once_with(spark._jsc.hadoopConfiguration())
        self.assertEqual([call.args[0].destination for call in fs.exists.call_args_list], paths)

    def test_preflight_lists_all_conflicting_outputs(self):
        paths = ["hdfs://master:9000/ratings", "hdfs://master:9000/books"]
        spark, fs = fake_spark(paths)
        with self.assertRaises(FileExistsError) as error:
            ensure_outputs_available(spark, paths)
        for path in paths:
            self.assertIn(path, str(error.exception))
        self.assertIn("--overwrite", str(error.exception))
        self.assertEqual(fs.exists.call_count, 2)

    def test_explicit_overwrite_allows_existing_output(self):
        spark, fs = fake_spark(["existing"])
        ensure_outputs_available(spark, ["existing"], overwrite=True)
        fs.exists.assert_called_once()

    def test_filesystem_failure_is_not_treated_as_absence(self):
        for overwrite in (False, True):
            with self.subTest(overwrite=overwrite):
                spark, fs = fake_spark()
                fs.exists.side_effect = OSError("NameNode unavailable")
                with self.assertRaisesRegex(OSError, "NameNode unavailable"):
                    ensure_outputs_available(spark, ["output"], overwrite=overwrite)

    def test_default_model_save_never_enables_overwrite(self):
        model = Mock()
        save_model(model, "new-model")
        model.write.return_value.overwrite.assert_not_called()
        model.write.return_value.save.assert_called_once_with("new-model")

    def test_explicit_model_overwrite_uses_authorized_writer(self):
        model = Mock()
        save_model(model, "existing-model", overwrite=True)
        writer = model.write.return_value
        writer.overwrite.assert_called_once_with()
        writer.overwrite.return_value.save.assert_called_once_with("existing-model")
        writer.save.assert_not_called()

    def test_parquet_uses_native_refusal_unless_authorized(self):
        for overwrite, mode in ((False, "errorifexists"), (True, "overwrite")):
            with self.subTest(overwrite=overwrite):
                frame = Mock()
                write_parquet(frame, "output", overwrite=overwrite)
                frame.write.mode.assert_called_once_with(mode)
                frame.write.mode.return_value.parquet.assert_called_once_with("output")

    def test_training_refuses_existing_models_before_read_or_fit(self):
        for name in ("train_als", "train_final"):
            with self.subTest(script=name):
                module = importlib.import_module(name)
                spark, _ = fake_spark([module.MODEL_PATH])
                with self.assertRaises(FileExistsError):
                    module.run(spark)
                spark.read.parquet.assert_not_called()

    def test_second_parquet_conflict_prevents_first_write(self):
        module = importlib.import_module("prepare_data")
        spark, fs = fake_spark([module.BOOKS_PATH])
        with patch.object(module, "write_parquet") as write, self.assertRaises(FileExistsError):
            module.run(spark)
        write.assert_not_called()
        spark.read.option.assert_not_called()
        self.assertEqual([call.args[0].destination for call in fs.exists.call_args_list],
                         [module.RATINGS_PATH, module.BOOKS_PATH])

    def test_main_forwards_authorization_and_stops_spark(self):
        for name in MODULES:
            module = importlib.import_module(name)
            for argv, overwrite in (([], False), (["--overwrite"], True)):
                with self.subTest(script=name, argv=argv):
                    spark, _ = fake_spark()
                    with patch.dict(sys.modules, fake_pyspark(spark)), patch.object(module, "run") as run:
                        module.main(argv)
                    run.assert_called_once_with(spark, overwrite=overwrite)
                    spark.stop.assert_called_once_with()

    def test_main_stops_spark_after_refusal(self):
        for name in MODULES:
            with self.subTest(script=name):
                module = importlib.import_module(name)
                existing = [module.BOOKS_PATH] if name == "prepare_data" else [module.MODEL_PATH]
                spark, _ = fake_spark(existing)
                with patch.dict(sys.modules, fake_pyspark(spark)), self.assertRaises(FileExistsError):
                    module.main([])
                spark.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
