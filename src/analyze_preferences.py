"""Analyze a user's rating history using precomputed topic features."""

import argparse
import json

from pyspark.sql import SparkSession

from preferences import analyze_user_preferences
from prepare_tags import HDFS_ROOT, ensure_new_output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--evidence-k", type=int, default=3)
    parser.add_argument("--hdfs-root", default=HDFS_ROOT)
    parser.add_argument("--topics-path", help="Existing per-book normalized topic Parquet")
    parser.add_argument("--output-root", help="Optional new HDFS directory for one JSON document")
    args = parser.parse_args()
    if not 1 <= args.user_id <= 2147483647:
        parser.error("--user-id must be a positive 32-bit integer")
    if args.top_k < 1 or args.evidence_k < 1:
        parser.error("--top-k and --evidence-k must be positive")
    root = args.hdfs_root.rstrip("/")
    topics_path = args.topics_path or f"{root}/processed/tag_features_v1/topics"
    spark = SparkSession.builder.appName(f"BookRec-Preferences-{args.user_id}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.output_root:
            ensure_new_output(spark, args.output_root)
        result = analyze_user_preferences(
            spark.read.parquet(f"{root}/processed/ratings"),
            spark.read.parquet(f"{root}/processed/books"),
            spark.read.parquet(topics_path),
            args.user_id, args.top_k, args.evidence_k,
        )
        result["sources"] = {"ratings": f"{root}/processed/ratings",
                             "books": f"{root}/processed/books", "topics": topics_path}
        payload = json.dumps(result, ensure_ascii=False, allow_nan=False, sort_keys=True)
        if args.output_root:
            spark.createDataFrame([(payload,)], ["value"]).coalesce(1).write.mode(
                "errorifexists"
            ).text(args.output_root)
            saved = spark.read.text(args.output_root).collect()
            if len(saved) != 1 or json.loads(saved[0]["value"]) != result:
                raise ValueError("Saved preference JSON differs from computed result")
        print(payload, flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
