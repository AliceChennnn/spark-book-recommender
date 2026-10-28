"""Streamlit interaction tests against the registered local demo snapshots."""

import unittest
from pathlib import Path
from unittest.mock import patch

from streamlit.testing.v1 import AppTest
import streamlit as st

from src.preferences import build_preferences
from src.web_backend import BackendError


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless((ROOT / "outputs/webui/demo_manifest.json").exists(), "Register demo snapshots first")
class WebUITest(unittest.TestCase):
    def app(self, page="数据概览"):
        app = AppTest.from_file(str(ROOT / "app.py"), default_timeout=15).run()
        if page != "数据概览":
            app.radio[0].set_value(page).run()
        self.assertFalse(app.exception)
        return app

    def test_overview_counts_and_images(self):
        app = self.app()
        self.assertEqual([m.value for m in app.metric], ["53,424", "10,000", "5,976,479"])
        self.assertEqual(len(app.get("image")), 3)
        self.assertEqual(len(app.dataframe), 3)

    def test_missing_overview_resources_are_warnings(self):
        st.cache_data.clear()
        empty = {"metrics": {"users": None, "books": None, "ratings": None},
                 "tables": {}, "figures": {}, "warnings": ["图片资源缺失：rating_distribution.png"]}
        try:
            with patch("src.web_backend.load_overview", return_value=empty):
                app = self.app()
                self.assertTrue(app.warning)
                self.assertEqual(app.metric[0].value, "暂无")
                self.assertFalse(app.exception)
        finally:
            st.cache_data.clear()

    def test_three_user_profiles_and_reruns_do_not_compute(self):
        with patch("src.web_backend.WebBackend._compute", side_effect=AssertionError("unexpected Spark job")):
            app = self.app("用户阅读偏好")
            for user_id, count in ((660, 132), (1000, 107), (2000, 148)):
                app.text_input[0].set_value(str(user_id))
                app.button[0].click().run()
                self.assertFalse(app.exception)
                self.assertEqual(app.metric[0].value, str(count))
                app.run()
                self.assertEqual(app.metric[0].value, str(count))

    def test_both_modes_and_unknown_fallback(self):
        with patch("src.web_backend.WebBackend._compute", side_effect=AssertionError("unexpected Spark job")):
            app = self.app("个性化图书推荐")
            for mode in ("ALS", "Popularity"):
                app.selectbox[0].set_value(mode)
                app.button[0].click().run()
                self.assertFalse(app.exception)
                self.assertEqual(len(app.dataframe[0].value), 10)
                label = "预测评分" if mode == "ALS" else "训练集评分次数"
                self.assertIn(label, app.dataframe[0].value.columns)
            app.text_input[0].set_value("999999")
            app.selectbox[0].set_value("ALS")
            app.button[0].click().run()
            self.assertEqual([m.value for m in app.metric], ["ALS", "Popularity"])
            self.assertTrue(any("回退" in w.value for w in app.warning))
            self.assertIn("训练集评分次数", app.dataframe[0].value.columns)
            app.text_input[0].set_value("661")
            app.button[0].click().run()
            self.assertGreater(app.dataframe[0].value["预测评分"].iloc[0], 5)

    def test_invalid_input_and_cache_miss_are_friendly(self):
        app = self.app("用户阅读偏好")
        for value in ("not-a-user", "999998"):
            app.text_input[0].set_value(value)
            app.button[0].click().run()
            self.assertTrue(app.error)
            self.assertFalse(app.exception)

    def test_spark_failure_and_no_high_ratings(self):
        app = self.app("用户阅读偏好")
        with patch("src.web_backend.WebBackend.get_preferences", side_effect=BackendError("Spark 暂时不可用")):
            app.button[0].click().run()
            self.assertIn("Spark 暂时不可用", app.error[0].value)
            self.assertFalse(app.exception)
        history = [{"user_id": 1, "book_id": 1, "rating": 3, "title": "Book", "authors": "A"}]
        envelope = {"result": build_preferences(1, history, []), "source": "computed", "generated_at": 1}
        with patch("src.web_backend.WebBackend.get_preferences", return_value=envelope):
            app.text_input[0].set_value("1")
            app.button[0].click().run()
            self.assertFalse(app.exception)
            self.assertTrue(any("没有评分不低于" in w.value for w in app.warning))
        envelope = {"result": build_preferences(999999, [], []), "source": "computed", "generated_at": 1}
        with patch("src.web_backend.WebBackend.get_preferences", return_value=envelope):
            app.text_input[0].set_value("999999")
            app.button[0].click().run()
            self.assertEqual(app.metric[0].value, "0")
            self.assertTrue(any("未找到" in w.value for w in app.warning))
            self.assertFalse(app.exception)


if __name__ == "__main__":
    unittest.main(verbosity=2)
