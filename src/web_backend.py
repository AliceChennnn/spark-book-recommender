"""Host-only file/cache adapter. PySpark runs exclusively through spark-submit."""

import csv
import hashlib
import json
import math
import os
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTAINER_ROOT = Path("/workspace/bookrec")
DATA_VERSION = "goodbooks10k-processed-v1-als_full_v1-topics_v1"
SCORE_TYPES = {"als": "als_predicted_rating", "popularity": "training_rating_count"}


class BackendError(RuntimeError):
    """An actionable error safe to display on the page."""


class CacheMiss(BackendError):
    pass


def parse_user_id(value):
    if type(value) is int:
        number = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]{1,10}", value.strip()):
        number = int(value.strip())
    else:
        raise BackendError("用户 ID 必须为正整数。")
    if not 1 <= number <= 2147483647:
        raise BackendError("用户 ID 必须在 1～2147483647 之间。")
    return number


def validate_payload(kind, payload, user_id, mode, top_k):
    """Check the file boundary before showing cached or computed data."""
    try:
        if payload["user_id"] != user_id:
            raise ValueError("wrong user")
        if kind == "preferences":
            profile = payload["profile"]
            assert profile["rated_book_count"] >= 0
            assert profile["high_rated_book_count"] >= 0
            assert profile["average_rating"] is None or math.isfinite(profile["average_rating"])
            assert [r["rating"] for r in profile["rating_distribution"]] == [1, 2, 3, 4, 5]
            assert sum(r["count"] for r in profile["rating_distribution"]) == profile["rated_book_count"]
            assert payload["parameters"]["top_k"] == top_k
            for field in ("topics", "authors", "representative_high_rated_books", "messages"):
                assert isinstance(payload[field], list)
            assert isinstance(payload["topic_summary"], dict)
        else:
            actual = payload["actual_algorithm"]
            assert payload["requested_algorithm"] == mode
            assert actual in SCORE_TYPES
            assert mode != "popularity" or actual == "popularity"
            if actual != mode:
                assert payload["fallback_reason"] == "unknown_als_user"
            rows = payload["recommendations"]
            assert payload["top_k"] == top_k
            assert len(rows) == payload["returned_count"] <= top_k
            assert len({r["book_id"] for r in rows}) == len(rows)
            assert [r["rank"] for r in rows] == list(range(1, len(rows) + 1))
            assert rows == sorted(rows, key=lambda r: (-r["score"], r["book_id"]))
            for row in rows:
                assert row["user_id"] == user_id and row["algorithm"] == actual
                assert row["score_type"] == SCORE_TYPES[actual] and math.isfinite(row["score"])
                assert "title" in row and "authors" in row
            assert isinstance(payload["messages"], list)
    except (KeyError, TypeError, ValueError, AssertionError) as exc:
        raise BackendError("结果文件缺失必要字段或与请求不一致，请重新生成该结果。") from exc
    return payload


def _read_json(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, allow_nan=False)
    os.replace(temporary, path)


class WebBackend:
    def __init__(self, root=PROJECT_ROOT, timeout=180, ttl=3600, runner=None):
        self.root = Path(root).resolve()
        self.timeout = timeout
        self.ttl = ttl
        self.runner = runner or subprocess.run
        self.data_version = os.environ.get("BOOKREC_DATA_VERSION", DATA_VERSION)
        self.work = self.root / "outputs" / "webui"
        self._job_lock = threading.Lock()

    def fingerprint(self):
        digest = hashlib.sha256(self.data_version.encode())
        for name in ("src/preferences.py", "src/recommenders.py", "src/prepare_tags.py",
                     "src/train_final.py", "src/web_job.py", "src/web_backend.py"):
            path = self.root / name
            digest.update(name.encode())
            digest.update(path.read_bytes() if path.is_file() else b"missing")
        return digest.hexdigest()

    def _key(self, kind, user_id, mode, top_k):
        return f"{kind}:{user_id}:{mode}:{top_k}"

    def prepare_demo_manifest(self):
        """Register already verified snapshots without launching any Spark job."""
        entries = {}
        for kind, users in (("preferences", (660, 1000, 2000)),
                            ("recommendations", (660, 1000, 2000, 661, 999999))):
            for user_id in users:
                for mode in (("",) if kind == "preferences" else ("als", "popularity")):
                    relative = (f"outputs/user_preferences_v1/user_{user_id}.json" if kind == "preferences"
                                else f"outputs/recommendation_modes_v1/user_{user_id}_{mode}.json")
                    path = self.root / relative
                    if not path.is_file():
                        continue
                    validate_payload(kind, _read_json(path), user_id, mode, 10)
                    entries[self._key(kind, user_id, mode, 10)] = {
                        "path": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "generated_at": path.stat().st_mtime,
                    }
        manifest = {"fingerprint": self.fingerprint(), "data_version": self.data_version, "entries": entries}
        _write_json(self.work / "demo_manifest.json", manifest)
        return len(entries)

    def _cached(self, kind, user_id, mode, top_k, fingerprint):
        key = self._key(kind, user_id, mode, top_k)
        cache_id = hashlib.sha256(f"{fingerprint}:{key}".encode()).hexdigest()
        cache_path = self.work / "cache" / f"{cache_id}.json"
        try:
            item = _read_json(cache_path)
            age = time.time() - item["generated_at"]
            if item["key"] == key and item["fingerprint"] == fingerprint and 0 <= age < self.ttl:
                validate_payload(kind, item["result"], user_id, mode, top_k)
                return {**item, "source": "runtime_cache"}, cache_path
        except (OSError, ValueError, KeyError, TypeError, BackendError):
            pass
        try:
            manifest = _read_json(self.work / "demo_manifest.json")
            if manifest["fingerprint"] != fingerprint or manifest["data_version"] != self.data_version:
                return None, cache_path
            entry = manifest["entries"][key]
            path = (self.root / entry["path"]).resolve()
            if not path.is_relative_to(self.root / "outputs"):
                return None, cache_path
            if hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
                return None, cache_path
            result = validate_payload(kind, _read_json(path), user_id, mode, top_k)
            return {"result": result, "source": "precomputed", "generated_at": entry["generated_at"]}, cache_path
        except (OSError, ValueError, KeyError, TypeError, BackendError):
            return None, cache_path

    def get_preferences(self, user_id, top_k=10, allow_compute=False):
        return self._get("preferences", user_id, "", top_k, allow_compute)

    def get_recommendations(self, user_id, mode="als", top_k=10, allow_compute=False):
        return self._get("recommendations", user_id, mode, top_k, allow_compute)

    def _get(self, kind, user_id, mode, top_k, allow_compute):
        user_id = parse_user_id(user_id)
        if type(top_k) is not int or not 1 <= top_k <= 50:
            raise BackendError("Top-K 必须为 1～50 的整数。")
        if kind == "recommendations" and mode not in SCORE_TYPES:
            raise BackendError("推荐模式必须为 ALS 或 Popularity。")
        fingerprint = self.fingerprint()
        cached, cache_path = self._cached(kind, user_id, mode, top_k, fingerprint)
        if cached:
            return cached
        if not allow_compute:
            raise CacheMiss("该请求没有有效的本地结果。请勾选“没有缓存时使用 Spark 计算”后再次提交。")
        if not self._job_lock.acquire(blocking=False):
            raise BackendError("已有 Spark 计算正在运行，请稍后重试；演示缓存仍可直接查看。")
        try:
            cached, cache_path = self._cached(kind, user_id, mode, top_k, fingerprint)
            if cached:
                return cached
            payload = self._compute(kind, user_id, mode, top_k)
            item = {"key": self._key(kind, user_id, mode, top_k), "fingerprint": fingerprint,
                    "generated_at": time.time(), "result": payload}
            try:
                _write_json(cache_path, item)
            except OSError as exc:
                raise BackendError("结果缓存无法写入，请检查 outputs/webui 的写入权限。") from exc
            return {**item, "source": "computed"}
        finally:
            self._job_lock.release()

    def _compute(self, kind, user_id, mode, top_k):
        job_dir = self.work / "jobs" / uuid.uuid4().hex
        try:
            job_dir.mkdir(parents=True, exist_ok=False)
        except OSError as exc:
            raise BackendError("无法创建结果目录，请检查 outputs/webui 的写入权限。") from exc
        output_path = job_dir / "result.json"
        container_path = CONTAINER_ROOT / output_path.relative_to(self.root)
        command = [
            "docker", "exec", "-w", str(CONTAINER_ROOT), "master", "/usr/bin/timeout",
            "--signal=TERM", "--kill-after=15s", f"{self.timeout}s",
            "/usr/local/spark/bin/spark-submit", "--master", "spark://master:7077",
            "--total-executor-cores", "2", "src/web_job.py",
            "--kind", kind, "--user-id", str(user_id), "--top-k", str(top_k),
            "--output-json", str(container_path),
        ]
        if kind == "recommendations":
            command.extend(["--mode", mode])
        try:
            with (job_dir / "spark.log").open("w", encoding="utf-8") as log:
                completed = self.runner(command, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=self.timeout + 30, check=False, shell=False)
        except subprocess.TimeoutExpired as exc:
            raise BackendError("计算超时，请稍后重试或先查看已缓存的演示用户。") from exc
        except OSError as exc:
            raise BackendError("无法调用 Docker，请确认容器已运行且当前用户有 Docker 访问权限。") from exc
        if completed.returncode in (124, 137):
            raise BackendError("计算超时，请稍后重试或先查看已缓存的演示用户。")
        if completed.returncode:
            raise BackendError("Spark 暂时不可用或作业未完成，请检查现有集群；已缓存结果仍可查看。")
        try:
            return validate_payload(kind, _read_json(output_path), user_id, mode, top_k)
        except (OSError, ValueError) as exc:
            raise BackendError("计算没有生成有效 JSON 结果，请检查作业日志后重试。") from exc


def asset_signature(root=PROJECT_ROOT):
    root = Path(root)
    paths = [root / "data/raw/books.csv"]
    paths += [root / "outputs/analysis" / f"{name}.csv" for name in
              ("rating_distribution", "popular_books", "high_rated_books", "user_activity")]
    paths += [root / "outputs/figures" / f"{name}.png" for name in
              ("rating_distribution", "popular_books", "user_activity")]
    return tuple((str(path), path.stat().st_mtime_ns, path.stat().st_size) if path.is_file()
                 else (str(path), None, None) for path in paths)


def load_overview(root=PROJECT_ROOT):
    """Read existing small CSV/PNG assets; never scan all rating CSVs or call Spark."""
    root = Path(root)
    result = {"metrics": {"users": None, "books": None, "ratings": None},
              "tables": {}, "figures": {}, "warnings": []}
    for name in ("rating_distribution", "popular_books", "high_rated_books", "user_activity"):
        try:
            with (root / "outputs/analysis" / f"{name}.csv").open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            if not rows:
                raise ValueError("empty CSV")
            result["tables"][name] = rows
            if name == "rating_distribution":
                result["metrics"]["ratings"] = sum(int(row["rating_count"]) for row in rows)
            elif name == "user_activity":
                result["metrics"]["users"] = sum(int(row["user_count"]) for row in rows)
        except (OSError, ValueError, KeyError, csv.Error):
            result["warnings"].append(f"缺少或无法读取统计资源：{name}.csv")
    try:
        with (root / "data/raw/books.csv").open(encoding="utf-8", newline="") as stream:
            result["metrics"]["books"] = len({row["book_id"] for row in csv.DictReader(stream)})
    except (OSError, ValueError, KeyError, csv.Error):
        result["warnings"].append("本地图书元数据不可用，暂时无法显示图书数量。")
    for name in ("rating_distribution", "popular_books", "user_activity"):
        path = root / "outputs/figures" / f"{name}.png"
        try:
            result["figures"][name] = path.read_bytes()
        except OSError:
            result["warnings"].append(f"图片资源缺失：{name}.png")
    return result
