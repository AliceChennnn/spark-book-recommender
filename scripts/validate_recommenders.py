"""Read-only cluster acceptance checks; save results only to a new local directory."""

import argparse
import json
import math
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recommenders import BookRecommender, RECOMMENDATION_FIELDS, SCORE_TYPES  # noqa: E402


def save_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, allow_nan=False, indent=2)


def check_result(result, seen, heldout):
    rows = result["recommendations"]
    ids = [row["book_id"] for row in rows]
    assert len(ids) == len(set(ids)) == result["returned_count"]
    assert len(ids) <= result["top_k"]
    assert not set(ids) & seen
    assert not set(ids) & heldout
    assert [row["rank"] for row in rows] == list(range(1, len(rows) + 1))
    assert rows == sorted(rows, key=lambda row: (-row["score"], row["book_id"]))
    for row in rows:
        assert set(row) == set(RECOMMENDATION_FIELDS)
        assert row["user_id"] == result["user_id"]
        assert row["algorithm"] == result["actual_algorithm"]
        assert row["score_type"] == SCORE_TYPES[result["actual_algorithm"]]
        assert math.isfinite(row["score"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="outputs/recommendation_modes_v1")
    args = parser.parse_args()
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)
    spark = SparkSession.builder.appName("BookRec-RecommendationValidation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    service = None
    reference = None
    try:
        service = BookRecommender.from_hdfs(spark)
        ratings = service.ratings
        # Independent copy of the unchanged official split for acceptance checks.
        test_condition = (
            (F.pmod(F.col("user_id"), F.lit(20)) != 0)
            & (F.pmod(F.xxhash64("user_id", "book_id"), F.lit(10)) == 0)
        )
        reference = ratings.filter(~test_condition).groupBy("book_id").agg(
            F.count("*").alias("expected_count")
        ).coalesce(4).cache()
        train_count = reference.agg(F.sum("expected_count")).first()[0]
        total_count = ratings.count()
        summary = {"split_counts": {"ratings": total_count, "train": train_count,
                                    "heldout": total_count - train_count}, "users": []}
        requests = {}
        for user_id in (660, 1000, 2000, 999999, 661):
            history = ratings.filter(F.col("user_id") == user_id).select(
                "book_id", test_condition.alias("heldout")
            ).collect()
            seen = {row.book_id for row in history}
            heldout = {row.book_id for row in history if row.heldout}
            results = {}
            for mode in ("als", "popularity"):
                result = service.recommend(user_id, 10, mode)
                check_result(result, seen, heldout)
                assert result["returned_count"] == 10
                save_json(output / f"user_{user_id}_{mode}.json", result)
                results[mode] = result
            if user_id == 999999:
                assert not seen
                assert results["als"]["actual_algorithm"] == "popularity"
                assert results["als"]["fallback_reason"] == "unknown_als_user"
                assert results["als"]["recommendations"] == results["popularity"]["recommendations"]
            else:
                assert results["als"]["actual_algorithm"] == "als"
            if user_id == 661:
                assert heldout
            ids_als = {r["book_id"] for r in results["als"]["recommendations"]}
            ids_popular = {r["book_id"] for r in results["popularity"]["recommendations"]}
            row = {"user_id": user_id, "seen_books": len(seen), "heldout_seen_books": len(heldout),
                   "already_rated_overlap": 0, "top10_overlap": len(ids_als & ids_popular),
                   "als_actual_algorithm": results["als"]["actual_algorithm"],
                   "als_top3": results["als"]["recommendations"][:3],
                   "popularity_top3": results["popularity"]["recommendations"][:3]}
            summary["users"].append(row)
            requests[user_id] = results
            print(json.dumps(row, ensure_ascii=False), flush=True)

        mismatch = service._get_popularity().join(reference, "book_id", "full_outer").filter(
            F.coalesce(F.col("popularity"), F.lit(-1))
            != F.coalesce(F.col("expected_count"), F.lit(-1))
        ).count()
        assert mismatch == 0
        summary["popularity_count_mismatches"] = mismatch

        # Reproduce the former CLI ranking with the same loaded model and inputs.
        seen = ratings.filter(F.col("user_id") == 660).select("book_id").distinct()
        candidates = service.books.join(seen, "book_id", "left_anti").withColumn("user_id", F.lit(660).cast("int"))
        legacy = service._get_model().transform(candidates).filter(
            F.col("prediction").isNotNull() & ~F.isnan("prediction")
        ).orderBy(F.desc("prediction"), F.asc("book_id")).limit(10).select("book_id", "prediction").collect()
        current = requests[660]["als"]["recommendations"]
        assert [r.book_id for r in legacy] == [r["book_id"] for r in current]
        assert all(math.isclose(old.prediction, new["score"], abs_tol=1e-7) for old, new in zip(legacy, current))
        summary["legacy_als_order_and_scores_match"] = True
        save_json(output / "summary.json", summary)
        print("All cluster acceptance checks passed.", flush=True)
    finally:
        if reference is not None:
            reference.unpersist()
        if service is not None:
            service.close()
        spark.stop()


if __name__ == "__main__":
    main()
