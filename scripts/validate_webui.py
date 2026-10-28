"""Exercise local demo caches and the real Docker JSON bridge from Ubuntu."""

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.web_backend import WebBackend  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", default="outputs/webui/validation.json")
    args = parser.parse_args()
    report_path = Path(args.report)
    if report_path.exists():
        raise FileExistsError(f"Report already exists: {report_path}; choose a new --report path")
    calls = []

    def runner(command, **kwargs):
        calls.append(command)
        return subprocess.run(command, **kwargs)

    backend = WebBackend(runner=runner)
    report = {"demo_profiles": {}, "demo_recommendations": {}, "on_demand": []}
    for user_id, count in ((660, 132), (1000, 107), (2000, 148)):
        envelope = backend.get_preferences(user_id)
        assert envelope["result"]["profile"]["rated_book_count"] == count
        report["demo_profiles"][user_id] = {"rated_book_count": count, "source": envelope["source"]}
    for user_id in (660, 1000, 2000, 999999):
        for mode in ("als", "popularity"):
            envelope = backend.get_recommendations(user_id, mode)
            result = envelope["result"]
            assert result["returned_count"] == 10
            if user_id == 999999 and mode == "als":
                assert result["actual_algorithm"] == "popularity"
                assert result["fallback_reason"] == "unknown_als_user"
            report["demo_recommendations"][f"{user_id}:{mode}"] = {
                "count": result["returned_count"], "actual_algorithm": result["actual_algorithm"],
                "source": envelope["source"],
            }
    assert not calls
    report["demo_subprocess_calls"] = 0
    requested = []
    for kind, user_id, mode in (("preferences", 662, ""), ("recommendations", 662, "als"),
                                 ("recommendations", 662, "popularity"), ("preferences", 999999, "")):
        start = time.monotonic()
        method = backend.get_preferences if kind == "preferences" else backend.get_recommendations
        args = (user_id,) if kind == "preferences" else (user_id, mode)
        before = len(calls)
        envelope = method(*args, allow_compute=True)
        first_calls = len(calls) - before
        repeated = method(*args, allow_compute=True)
        assert repeated["source"] == "runtime_cache"
        assert repeated["result"] == envelope["result"]
        assert len(calls) - before == first_calls
        result = envelope["result"]
        if kind == "recommendations":
            assert result["returned_count"] == 10
        if user_id == 999999:
            assert result["status"] == "user_not_found"
            assert result["profile"]["average_rating"] is None
        row = {"kind": kind, "user_id": user_id, "mode": mode, "source": envelope["source"],
               "spark_jobs": first_calls, "seconds": round(time.monotonic() - start, 3),
               "repeat_additional_jobs": 0}
        report["on_demand"].append(row)
        requested.append((kind, mode, result))
        print(json.dumps(row), flush=True)
    seen = set()
    with (backend.root / "data/raw/ratings.csv").open(newline="") as stream:
        for row in csv.DictReader(stream):
            if row["user_id"] == "662":
                seen.add(int(row["book_id"]))
    for kind, mode, result in requested:
        if result["user_id"] == 662:
            if kind == "recommendations":
                assert not seen & {r["book_id"] for r in result["recommendations"]}
            else:
                assert result["profile"]["rated_book_count"] == len(seen)
    report["on_demand_seen_overlap"] = 0
    report["host_imported_pyspark"] = "pyspark" in sys.modules
    assert not report["host_imported_pyspark"]
    report["windows_access"] = "Manual browser check; see docs/webui.md for this run's confirmation"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
    print("WebUI local cache and real Spark bridge checks passed.", flush=True)


if __name__ == "__main__":
    main()
