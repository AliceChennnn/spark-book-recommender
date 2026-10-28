"""spark-submit --master local[1]; only new file:///tmp fixtures, no ALS fit or HDFS writes."""

import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from output_safety import ensure_outputs_available, save_model, write_parquet  # noqa: E402


def digests(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file()}


class SparkOutputSafetyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("BookRec-OutputSafetyTests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_parquet_refusal_race_and_authorized_replacement(self):
        with tempfile.TemporaryDirectory(prefix="bookrec-safety-") as directory:
            target = Path(directory) / "ratings"
            uri = target.as_uri()
            ensure_outputs_available(self.spark, [uri])
            write_parquet(self.spark.range(2).coalesce(1), uri)
            before = digests(target)
            with self.assertRaises(FileExistsError):
                ensure_outputs_available(self.spark, [uri])
            # The target appeared after the first check; native save must still refuse.
            with self.assertRaisesRegex(Exception, "(?i)already exists"):
                write_parquet(self.spark.range(3).coalesce(1), uri)
            self.assertEqual(digests(target), before)
            ensure_outputs_available(self.spark, [uri], overwrite=True)
            write_parquet(self.spark.range(3).coalesce(1), uri, overwrite=True)
            self.assertEqual(self.spark.read.parquet(uri).count(), 3)

    def test_model_refusal_race_and_authorized_replacement(self):
        with tempfile.TemporaryDirectory(prefix="bookrec-safety-") as directory:
            target = Path(directory) / "model"
            uri = target.as_uri()
            # An empty pipeline exercises Spark MLWriter without fitting any model.
            model = PipelineModel(stages=[])
            ensure_outputs_available(self.spark, [uri])
            save_model(model, uri)
            before = digests(target)
            with self.assertRaises(FileExistsError):
                ensure_outputs_available(self.spark, [uri])
            with self.assertRaisesRegex(Exception, "(?i)already exists"):
                save_model(model, uri)
            self.assertEqual(digests(target), before)
            ensure_outputs_available(self.spark, [uri], overwrite=True)
            save_model(model, uri, overwrite=True)
            self.assertEqual(PipelineModel.load(uri).stages, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
