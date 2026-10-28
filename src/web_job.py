"""Container bridge: invoke existing algorithms and write one dedicated JSON file."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from preferences import analyze_user_preferences
from recommenders import BookRecommender, HDFS_ROOT, validate_request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=("preferences", "recommendations"), required=True)
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--mode", choices=("als", "popularity"), default="als")
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()
    try:
        validate_request(args.user_id, args.top_k, args.mode)
        if args.top_k > 50:
            raise ValueError("top_k must not exceed 50")
    except ValueError as exc:
        parser.error(str(exc))
    output = Path(args.output_json).resolve()
    jobs = Path(__file__).resolve().parents[1] / "outputs/webui/jobs"
    if not output.is_relative_to(jobs.resolve()) or not output.parent.is_dir() or output.exists():
        parser.error("output must be a new file inside an existing outputs/webui/jobs request directory")
    spark = SparkSession.builder.appName(f"BookRec-Web-{args.kind}-{args.user_id}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    service = None
    try:
        if args.kind == "recommendations":
            service = BookRecommender.from_hdfs(spark)
            result = service.recommend(args.user_id, args.top_k, args.mode)
        else:
            result = analyze_user_preferences(
                spark.read.parquet(f"{HDFS_ROOT}/processed/ratings"),
                spark.read.parquet(f"{HDFS_ROOT}/processed/books"),
                spark.read.parquet(f"{HDFS_ROOT}/processed/tag_features_v1/topics"),
                args.user_id, args.top_k,
            )
        temporary = output.with_suffix(".tmp")
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, ensure_ascii=False, allow_nan=False)
        temporary.replace(output)
    finally:
        if service is not None:
            service.close()
        spark.stop()


if __name__ == "__main__":
    main()
