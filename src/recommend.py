
import argparse
import json

from pyspark.sql import SparkSession

from recommenders import BookRecommender, HDFS_ROOT, MODEL_PATH


def display_result(result, output_format="table"):
    if output_format == "json":
        print(json.dumps(result, ensure_ascii=False, allow_nan=False), flush=True)
        return
    print("请求算法:", result["requested_algorithm"])
    print("实际使用算法:", result["actual_algorithm"])
    for message in result["messages"]:
        print(message)
    print(f"\n===== Recommendations for User {result['user_id']} =====")
    score_label = "predicted_rating" if result["actual_algorithm"] == "als" else "rating_count"
    print(f"{'rank':>4} {'book_id':>7}  {'title':55} {'authors':40} {score_label}")
    for row in result["recommendations"]:
        title, authors = row["title"] or "", row["authors"] or ""
        print(f"{row['rank']:>4} {row['book_id']:>7}  {title[:55]:55} {authors[:40]:40} {row['score']:.3f}")


def recommend(spark, user_id, top_k, mode="als", output_format="table", service=None):
    """Preserve the old three-argument entry point and return structured results."""
    owned = service is None
    service = service or BookRecommender.from_hdfs(spark)
    try:
        result = service.recommend(user_id, top_k, mode)
        display_result(result, output_format)
        return result
    finally:
        if owned:
            service.close()


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--mode", choices=("als", "popularity"), default="als")
    parser.add_argument("--format", choices=("table", "json"), default="table")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not 1 <= args.user_id <= 2147483647:
        parser.error("--user-id must be a positive 32-bit integer")
    if args.top_k < 1:
        parser.error("--top-k must be positive")

    spark = (
        SparkSession.builder
        .appName("BookRec-Recommendation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        recommend(spark, args.user_id, args.top_k, args.mode, args.format)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
