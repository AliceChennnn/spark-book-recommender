"""Read-only RMSE audit of the saved model; never fit or save an ALS model."""

import argparse
import hashlib
import json
from pathlib import Path

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recommenders import HDFS_ROOT, final_test_condition  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", required=True, help="New local file inside the bind mount")
    args = parser.parse_args()
    output = Path(args.output_json)
    if output.exists() or not output.parent.is_dir():
        parser.error("output must be a new file in an existing directory")
    spark = SparkSession.builder.appName("BookRec-FrozenModelAudit").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cached = []
    try:
        ratings = spark.read.parquet(f"{HDFS_ROOT}/processed/ratings")
        for name in ("user_id", "book_id"):
            if ratings.schema[name].dataType.simpleString() != "int":
                raise TypeError("Hash inputs must retain the original INT schema")
        test_condition = final_test_condition()
        train = ratings.filter(~test_condition)
        test = ratings.filter(test_condition)
        stats = train.groupBy("book_id").agg(
            F.count("*").alias("n"), F.sum("rating").alias("total_rating")
        ).coalesce(4).cache()
        cached.append(stats)
        totals = stats.agg(F.sum("n"), F.sum("total_rating")).first()
        train_count, global_mean = totals[0], totals[1] / totals[0]
        test_count = test.count()
        model_path = f"{HDFS_ROOT}/models/als_full_v1"
        model = ALSModel.load(model_path)
        model.setColdStartStrategy("drop")
        predictions = model.transform(test).cache()
        cached.append(predictions)
        valid_count = predictions.count()
        if not valid_count or not test_count:
            raise ValueError("No valid heldout predictions")
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        als_rmse = evaluator.evaluate(predictions)
        means = stats.select("book_id", (F.col("total_rating") / F.col("n")).alias("book_mean"))
        baseline = predictions.select("book_id", "rating").join(means, "book_id", "left").withColumn(
            "prediction", F.coalesce(F.col("book_mean"), F.lit(global_mean))
        )
        baseline_rmse = evaluator.evaluate(baseline)
        source = Path(__file__).resolve().parents[1] / "src/train_final.py"
        report = {
            "audit_kind": "read_only_remeasurement_of_frozen_model", "spark_version": spark.version,
            "model_path": model_path, "rank_from_loaded_model": model.rank,
            "train_final_source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "train_rows": train_count, "test_rows": test_count,
            "valid_predictions": valid_count, "prediction_coverage": valid_count / test_count,
            "als_rmse": als_rmse, "item_mean_rmse_same_valid_rows": baseline_rmse,
            "relative_rmse_reduction": (baseline_rmse - als_rmse) / baseline_rmse,
            "baseline_scope": "item means from train only, evaluated on ALS-valid test rows",
            "split": "pmod(user_id,20)!=0 AND pmod(xxhash64(user_id,book_id),10)=0; INT IDs",
        }
        with output.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2, allow_nan=False)
        print(json.dumps(report, ensure_ascii=False), flush=True)
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
