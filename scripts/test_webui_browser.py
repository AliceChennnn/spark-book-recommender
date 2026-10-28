"""Optional real-browser smoke test against an already running Streamlit server."""

import argparse
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8501")
    args = parser.parse_args()
    output = Path(__file__).resolve().parents[1] / "outputs/webui/browser"
    output.mkdir(parents=True, exist_ok=True)
    errors = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(args.url)
        expect(page.get_by_role("heading", name="数据概览", exact=True)).to_be_visible()
        expect(page.locator("img")).to_have_count(3)
        for image in page.locator("img").all():
            image.scroll_into_view_if_needed()
            expect(image).to_be_visible()
            assert image.evaluate("img => img.complete && img.naturalWidth > 0")
        page.get_by_role("heading", name="数据概览", exact=True).scroll_into_view_if_needed()
        page.screenshot(path=str(output / "overview.png"), full_page=True)
        page.get_by_text("用户阅读偏好", exact=True).click()
        page.get_by_role("button", name="查看阅读偏好", exact=True).click()
        expect(page.get_by_test_id("stMetricValue").first).to_have_text("132")
        expect(page.get_by_text("偏好主题 Top 10", exact=True)).to_be_visible()
        page.get_by_role("heading", name="用户阅读偏好", exact=True).scroll_into_view_if_needed()
        page.screenshot(path=str(output / "preferences.png"), full_page=True)
        page.get_by_text("个性化图书推荐", exact=True).click()
        page.get_by_role("button", name="获取 Top 10 推荐", exact=True).click()
        expect(page.get_by_test_id("stMetricValue").nth(1)).to_have_text("ALS")
        with page.expect_download() as download_info:
            page.get_by_role("button", name="下载结果 JSON", exact=True).click()
        payload = json.loads(Path(download_info.value.path()).read_text())
        assert payload["returned_count"] == 10
        assert payload["recommendations"][0]["score_type"] == "als_predicted_rating"
        page.get_by_role("combobox").click()
        page.get_by_role("option", name="Popularity", exact=True).click()
        page.get_by_role("button", name="获取 Top 10 推荐", exact=True).click()
        expect(page.get_by_test_id("stMetricValue").nth(1)).to_have_text("Popularity")
        page.get_by_role("textbox", name="用户 ID", exact=True).fill("999999")
        page.get_by_role("combobox").click()
        page.get_by_role("option", name="ALS", exact=True).click()
        page.get_by_role("button", name="获取 Top 10 推荐", exact=True).click()
        expect(page.get_by_text("用户 999999 不在 ALS 模型中，已回退到 Popularity。", exact=True)).to_be_visible()
        expect(page.get_by_test_id("stMetricValue").nth(1)).to_have_text("Popularity")
        page.get_by_role("heading", name="个性化图书推荐", exact=True).scroll_into_view_if_needed()
        page.screenshot(path=str(output / "fallback.png"), full_page=True)
        assert not errors, errors
        browser.close()
    (output / "result.json").write_text(json.dumps({"url": args.url, "passed": True,
                                                    "javascript_errors": errors}, indent=2))
    print("Browser images, navigation, JSON download, modes and fallback passed.")


if __name__ == "__main__":
    main()
