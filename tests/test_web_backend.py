"""Host-only tests: no Docker, Spark or dataset required."""

import json
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from src.web_backend import BackendError, CacheMiss, CONTAINER_ROOT, WebBackend, load_overview, parse_user_id


def recommendation(user_id=7):
    return {"user_id": user_id, "requested_algorithm": "als", "actual_algorithm": "als",
            "fallback_reason": None, "top_k": 10, "returned_count": 1, "messages": [],
            "recommendations": [{"user_id": user_id, "book_id": 1, "title": "Book", "authors": "A, B",
                                 "rank": 1, "score": 5.2, "score_type": "als_predicted_rating", "algorithm": "als"}]}


class WebBackendTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.runner = Mock(side_effect=self.run_job)
        self.backend = WebBackend(self.root, runner=self.runner)

    def tearDown(self):
        self.directory.cleanup()

    def run_job(self, command, **kwargs):
        path = self.root / Path(command[command.index("--output-json") + 1]).relative_to(CONTAINER_ROOT)
        path.write_text(json.dumps(recommendation(int(command[command.index("--user-id") + 1]))))
        return subprocess.CompletedProcess(command, 0)

    def test_strict_inputs_never_start_subprocess(self):
        for value in ("1; echo bad", "1.0", "-1", "0", "99999999999", True, "１２"):
            with self.subTest(value=value), self.assertRaises(BackendError):
                self.backend.get_recommendations(value, allow_compute=True)
        for top_k in (0, 51, 1.5, True):
            with self.assertRaises(BackendError):
                self.backend.get_recommendations(1, top_k=top_k, allow_compute=True)
        self.assertEqual(parse_user_id(" 660 "), 660)
        self.runner.assert_not_called()

    def test_cache_miss_requires_explicit_compute(self):
        with self.assertRaises(CacheMiss):
            self.backend.get_recommendations(7)
        self.runner.assert_not_called()

    def test_array_command_result_file_and_repeated_cache(self):
        first = self.backend.get_recommendations(7, allow_compute=True)
        second = self.backend.get_recommendations(7, allow_compute=True)
        self.assertEqual(first["source"], "computed")
        self.assertEqual(second["source"], "runtime_cache")
        self.assertEqual(second["result"]["recommendations"][0]["score"], 5.2)
        self.assertEqual(self.runner.call_count, 1)
        command = self.runner.call_args.args[0]
        self.assertIsInstance(command, list)
        self.assertIn("/usr/bin/timeout", command)
        self.assertFalse(self.runner.call_args.kwargs["shell"])
        self.assertEqual(self.runner.call_args.kwargs["timeout"], 210)

    def test_timeout_and_unavailable_docker(self):
        for failure in (subprocess.TimeoutExpired(["docker"], 210), FileNotFoundError("docker")):
            self.runner.side_effect = failure
            with self.assertRaises(BackendError):
                self.backend.get_recommendations(7, allow_compute=True)
            self.assertFalse(self.backend._job_lock.locked())

    def test_failed_job_and_missing_or_wrong_json(self):
        for result in (subprocess.CompletedProcess([], 124), subprocess.CompletedProcess([], 1),
                       subprocess.CompletedProcess([], 0)):
            self.runner.side_effect = None
            self.runner.return_value = result
            with self.assertRaises(BackendError):
                self.backend.get_recommendations(7, allow_compute=True)

    def test_cache_expiry_and_version_invalidation(self):
        self.backend.get_recommendations(7, allow_compute=True)
        path = next((self.backend.work / "cache").glob("*.json"))
        item = json.loads(path.read_text())
        item["generated_at"] = time.time() - 3601
        path.write_text(json.dumps(item))
        with self.assertRaises(CacheMiss):
            self.backend.get_recommendations(7)
        self.backend.get_recommendations(7, allow_compute=True)
        self.backend.data_version = "new-data-version"
        with self.assertRaises(CacheMiss):
            self.backend.get_recommendations(7)

    def test_existing_snapshot_does_not_call_docker(self):
        path = self.root / "outputs/recommendation_modes_v1/user_660_als.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(recommendation(660)))
        self.assertEqual(self.backend.prepare_demo_manifest(), 1)
        self.assertEqual(self.backend.get_recommendations(660)["source"], "precomputed")
        self.runner.assert_not_called()
        path.write_text("{}")
        with self.assertRaises(CacheMiss):
            self.backend.get_recommendations(660)

    def test_busy_backend_and_missing_assets(self):
        self.backend._job_lock.acquire()
        try:
            with self.assertRaisesRegex(BackendError, "正在运行"):
                self.backend.get_recommendations(7, allow_compute=True)
        finally:
            self.backend._job_lock.release()
        overview = load_overview(self.root)
        self.assertIsNone(overview["metrics"]["users"])
        self.assertTrue(overview["warnings"])
        self.assertFalse(overview["figures"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
