"""Local Streamlit presentation; no PySpark imports in this process."""

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from src.web_backend import BackendError, WebBackend, asset_signature, load_overview


st.set_page_config(page_title="Spark 图书推荐", page_icon="📚", layout="wide")


@st.cache_resource
def get_backend():
    return WebBackend()


@st.cache_data(max_entries=4)
def overview_data(signature):
    return load_overview()


def source_caption(envelope):
    label = {"precomputed": "预计算演示结果", "runtime_cache": "按需计算缓存", "computed": "本次 Spark 计算"}
    timestamp = datetime.fromtimestamp(envelope["generated_at"], ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M")
    st.caption(f"用户 {envelope['result']['user_id']} · {label[envelope['source']]} · 生成时间 {timestamp}（北京时间）")


def download_result(payload, name):
    st.download_button("下载结果 JSON", json.dumps(payload, ensure_ascii=False, indent=2),
                       file_name=f"{name}_{payload['user_id']}.json", mime="application/json")


def overview():
    st.header("数据概览")
    st.caption("复用已有统计和 Matplotlib 图表；查看本页不启动 Spark 作业。")
    data = overview_data(asset_signature())
    for column, label, key in zip(st.columns(3), ("用户数", "图书数", "评分记录数"), ("users", "books", "ratings")):
        value = data["metrics"][key]
        column.metric(label, f"{value:,}" if value is not None else "暂无")
    for warning in data["warnings"]:
        st.warning(warning)
    names = {"rating_distribution": "评分分布", "popular_books": "热门图书", "user_activity": "用户评分活跃度"}
    for name, label in names.items():
        st.subheader(label)
        if name in data["figures"]:
            try:
                st.image(data["figures"][name], width="stretch")
            except Exception:
                st.warning(f"{label}图片无法显示，可查看下方统计表。")
        if name in data["tables"]:
            labels = {"book_id": "图书 ID", "title": "书名", "authors": "作者署名",
                      "rating": "评分", "rating_count": "评分次数", "avg_rating": "平均评分",
                      "activity_group": "评分数量区间", "user_count": "用户数"}
            st.dataframe(pd.DataFrame(data["tables"][name]).rename(columns=labels), hide_index=True, width="stretch")


def preferences_page():
    st.header("用户阅读偏好")
    st.info("偏好由评分历史推断，评分记录不代表用户已实际读完这些图书。")
    st.caption("演示用户：660、1000、2000。其他用户可按需计算，首次通常需要数十秒。")
    with st.form("preferences_form"):
        user_id = st.text_input("用户 ID", "660", key="pref_user_id")
        allow = st.checkbox("没有缓存时使用 Spark 计算", key="pref_allow")
        submitted = st.form_submit_button("查看阅读偏好")
    if submitted:
        try:
            with st.spinner("正在读取或计算阅读画像，请稍候…"):
                st.session_state["pref_result"] = get_backend().get_preferences(user_id, allow_compute=allow)
        except BackendError as exc:
            st.session_state.pop("pref_result", None)
            st.error(str(exc))
    envelope = st.session_state.get("pref_result")
    if not envelope:
        return
    source_caption(envelope)
    result = envelope["result"]
    for message in result["messages"]:
        st.warning(message)
    profile = result["profile"]
    columns = st.columns(3)
    columns[0].metric("评分图书数量", profile["rated_book_count"])
    columns[1].metric("平均评分", f"{profile['average_rating']:.3f}" if profile["average_rating"] is not None else "暂无")
    columns[2].metric("高分图书数量（≥4 分）", profile["high_rated_book_count"])
    st.subheader("历史评分分布")
    distribution = pd.DataFrame(profile["rating_distribution"]).rename(columns={"rating": "评分", "count": "图书数量"})
    st.bar_chart(distribution.set_index("评分"))
    st.subheader("偏好主题 Top 10")
    if result["topics"]:
        topics = pd.DataFrame([{"主题": t["tag_name"], "偏好得分": t["preference_score"],
                                "得分占比（%）": t["score_share"] * 100,
                                "支持图书数": t["supporting_book_count"]} for t in result["topics"]])
        st.dataframe(topics, hide_index=True, width="stretch")
        st.caption("得分占比表示高分图书贡献份额，不是真实兴趣概率；同义标签尚未自动合并。")
        with st.expander("查看主题的主要贡献图书"):
            for topic in result["topics"]:
                st.write(topic["tag_name"])
                st.dataframe(pd.DataFrame(topic["top_contributing_books"]).rename(columns={
                    "book_id": "图书 ID", "title": "书名", "authors": "作者署名", "rating": "用户评分", "contribution": "贡献分数"
                }), hide_index=True, width="stretch")
    else:
        st.info("暂无足够的高分主题证据。")
    st.subheader("偏好作者署名")
    if result["authors"]:
        st.dataframe(pd.DataFrame([{"作者署名组合": a["authors"], "支持图书数": a["supporting_book_count"],
                                    "平均评分": a["average_rating"]} for a in result["authors"]]), hide_index=True, width="stretch")
        st.caption("保留原始合作署名组合，不按逗号拆分作者。")
    else:
        st.info("暂无高分作者证据。")
    st.subheader("代表性高分图书")
    if result["representative_high_rated_books"]:
        st.dataframe(pd.DataFrame(result["representative_high_rated_books"]).rename(columns={
            "book_id": "图书 ID", "title": "书名", "authors": "作者署名", "rating": "用户评分"
        }), hide_index=True, width="stretch")
    download_result(result, "preferences")


def recommendations_page():
    st.header("个性化图书推荐")
    st.caption("演示缓存：660、1000、2000、661、999999；其余请求需明确启用按需计算。")
    with st.form("recommendations_form"):
        user_id = st.text_input("用户 ID", "660", key="rec_user_id")
        mode = st.selectbox("请求算法", ("ALS", "Popularity"), key="rec_mode")
        allow = st.checkbox("没有缓存时使用 Spark 计算", key="rec_allow")
        submitted = st.form_submit_button("获取 Top 10 推荐")
    if submitted:
        try:
            with st.spinner("正在读取或生成推荐，请稍候…"):
                st.session_state["rec_result"] = get_backend().get_recommendations(user_id, mode.lower(), allow_compute=allow)
        except BackendError as exc:
            st.session_state.pop("rec_result", None)
            st.error(str(exc))
    envelope = st.session_state.get("rec_result")
    if not envelope:
        return
    source_caption(envelope)
    result = envelope["result"]
    labels = {"als": "ALS", "popularity": "Popularity"}
    columns = st.columns(2)
    columns[0].metric("请求算法", labels[result["requested_algorithm"]])
    columns[1].metric("实际执行算法", labels[result["actual_algorithm"]])
    for message in result["messages"]:
        st.warning(message)
    actual = result["actual_algorithm"]
    score_label = "预测评分" if actual == "als" else "训练集评分次数"
    if result["recommendations"]:
        frame = pd.DataFrame(result["recommendations"])[["rank", "book_id", "title", "authors", "score"]].rename(columns={
            "rank": "排名", "book_id": "图书 ID", "title": "书名", "authors": "作者署名", "score": score_label})
        st.dataframe(frame, hide_index=True, width="stretch", column_config={
            score_label: st.column_config.NumberColumn(format="%.6f" if actual == "als" else "%d")})
    else:
        st.info("没有可推荐的未评分图书。")
    st.caption("排除用户全部已评分图书。预测评分保留模型原值，可能超出 1～5；热门分数仅为训练集评分次数。")
    download_result(result, f"recommendations_{result['requested_algorithm']}")


st.title("图书推荐与阅读偏好分析")
st.caption("Goodbooks-10k · Spark SQL · MLlib ALS")
page = st.sidebar.radio("功能", ("数据概览", "用户阅读偏好", "个性化图书推荐"), key="page")
st.sidebar.caption("预计算结果可离线演示；按需计算需要现有 Spark / HDFS 集群可用。")
{"数据概览": overview, "用户阅读偏好": preferences_page, "个性化图书推荐": recommendations_page}[page]()
