"""Host-only tests: no Docker, Spark or dataset required."""

import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.web_backend import BackendError, CacheMiss, CONTAINER_ROOT, DATA_VERSION, WebBackend, load_overview, parse_user_id


def recommendation(user_id=7):
    return {"user_id": user_id, "requested_algorithm": "als", "actual_algorithm": "als",
            "fallback_reason": None, "top_k": 10, "returned_count": 1, "messages": [],
            "recommendations": [{"user_id": user_id, "book_id": 1, "title": "Book", "authors": "A, B",
                                 "rank": 1, "score": 5.2, "score_type": "als_predicted_rating", "algorithm": "als"}]}


class WebBackendTest(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {"BOOKREC_DATA_VERSION": DATA_VERSION})
        environment.start()
        self.addCleanup(environment.stop)
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

    def test_unset_version_preserves_shipped_default(self):
        with patch.dict(os.environ):
            os.environ.pop("BOOKREC_DATA_VERSION", None)
            backend = WebBackend(self.root, runner=self.runner)
        self.assertEqual(backend.data_version, DATA_VERSION)
        self.assertEqual(backend.ttl, 3600)
        self.runner.assert_not_called()

    def test_environment_version_is_read_once_per_backend(self):
        with patch.dict(os.environ, {"BOOKREC_DATA_VERSION": "dataset-v1-als-a"}):
            backend = WebBackend(self.root, runner=self.runner)
        with patch.dict(os.environ, {"BOOKREC_DATA_VERSION": "dataset-v1-als-b"}):
            replacement = WebBackend(self.root, runner=self.runner)
            self.assertEqual(backend.data_version, "dataset-v1-als-a")
        self.assertEqual(replacement.data_version, "dataset-v1-als-b")
        self.runner.assert_not_called()

    def test_blank_version_is_rejected(self):
        for version in ("", " ", "\t\n"):
            with self.subTest(version=version), patch.dict(os.environ, {"BOOKREC_DATA_VERSION": version}):
                with self.assertRaisesRegex(BackendError, "BOOKREC_DATA_VERSION"):
                    WebBackend(self.root, runner=self.runner)
        self.runner.assert_not_called()

    def test_same_version_reuses_disk_cache_in_new_backend(self):
        self.backend.get_recommendations(7, allow_compute=True)
        other = WebBackend(self.root, runner=self.runner)
        self.assertEqual(other.get_recommendations(7)["source"], "runtime_cache")
        self.assertEqual(self.runner.call_count, 1)

    def test_dataset_data_and_model_changes_isolate_runtime_and_demo_cache(self):
        path = self.root / "outputs/recommendation_modes_v1/user_660_als.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(recommendation(660)))
        self.backend.prepare_demo_manifest()
        self.backend.get_recommendations(7, allow_compute=True)
        original_files = {p: p.read_bytes() for p in (self.root / "outputs").rglob("*.json")}
        versions = ("otherbooks-processed-v1-als_full_v1-topics_v1",
                    "goodbooks10k-processed-v2-als_full_v1-topics_v1",
                    "goodbooks10k-processed-v1-als_full_v2-topics_v1")
        self.runner.reset_mock()
        for version in versions:
            with self.subTest(version=version), patch.dict(os.environ, {"BOOKREC_DATA_VERSION": version}):
                backend = WebBackend(self.root, runner=self.runner)
                self.assertNotEqual(backend.fingerprint(), self.backend.fingerprint())
                for uid in (7, 660):
                    with self.assertRaises(CacheMiss):
                        backend.get_recommendations(uid)
        self.assertEqual({p: p.read_bytes() for p in original_files}, original_files)
        self.assertEqual(self.backend.get_recommendations(7)["source"], "runtime_cache")
        self.assertEqual(self.backend.get_recommendations(660)["source"], "precomputed")
        self.runner.assert_not_called()

    def test_new_version_cache_does_not_replace_old_namespace(self):
        self.backend.get_recommendations(7, allow_compute=True)
        original = next((self.backend.work / "cache").glob("*.json"))
        original_bytes = original.read_bytes()
        with patch.dict(os.environ, {"BOOKREC_DATA_VERSION": "futurebooks-v1-als-a"}):
            backend = WebBackend(self.root, runner=self.runner)
        self.assertEqual(backend.get_recommendations(7, allow_compute=True)["source"], "computed")
        self.assertEqual(backend.get_recommendations(7)["source"], "runtime_cache")
        self.assertEqual(original.read_bytes(), original_bytes)
        self.assertEqual(len(list((self.backend.work / "cache").glob("*.json"))), 2)
        self.assertEqual(self.backend.get_recommendations(7)["source"], "runtime_cache")
        self.assertEqual(self.runner.call_count, 2)

    def test_v1_snapshots_cannot_be_relabelled_to_new_version(self):
        path = self.root / "outputs/recommendation_modes_v1/user_660_als.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(recommendation(660)))
        self.backend.prepare_demo_manifest()
        manifest = self.backend.work / "demo_manifest.json"
        original = manifest.read_bytes()
        with patch.dict(os.environ, {"BOOKREC_DATA_VERSION": "futurebooks-v1-als-a"}):
            backend = WebBackend(self.root, runner=self.runner)
        with self.assertRaisesRegex(BackendError, "v1 演示快照"):
            backend.prepare_demo_manifest()
        self.assertEqual(manifest.read_bytes(), original)
        self.assertEqual(self.backend.get_recommendations(660)["source"], "precomputed")
        self.runner.assert_not_called()

    def test_code_fingerprint_change_still_invalidates_cache(self):
        source = self.root / "src/recommenders.py"
        source.parent.mkdir()
        source.write_text("# initial implementation\n")
        self.backend.get_recommendations(7, allow_compute=True)
        source.write_text("# changed implementation\n")
        with self.assertRaises(CacheMiss):
            self.backend.get_recommendations(7)
        self.assertEqual(self.runner.call_count, 1)

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
