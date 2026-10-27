"""Reusable summaries of rating history, not evidence of completed reading."""

import math
from collections import defaultdict


METHOD_VERSION = "rating-history-v1"
ANALYSIS_BASIS = "基于评分历史的偏好分析；评分记录不代表用户已实际读完这些图书。"


def _book_details(row):
    return {key: row.get(key) for key in ("book_id", "title", "authors", "rating")}


def _validate_history(user_id, history):
    seen = set()
    for row in history:
        if row.get("user_id") != user_id:
            raise ValueError("History must contain only the requested user")
        if not isinstance(row.get("book_id"), int) or row["book_id"] <= 0:
            raise ValueError("Invalid book_id in rating history")
        if not isinstance(row.get("rating"), int) or not 1 <= row["rating"] <= 5:
            raise ValueError("Ratings must be integers from 1 to 5")
        if row["book_id"] in seen:
            raise ValueError("Duplicate book_id in user history or books metadata")
        seen.add(row["book_id"])


def _topic_preferences(positive, features, top_k, evidence_k):
    """Keep per-book weight mass and aggregate exact names, without synonym NLP."""
    seen = set()
    weights = defaultdict(list)
    contributions = defaultdict(lambda: defaultdict(list))
    tag_ids = defaultdict(set)
    for row in features:
        book_id = row["book_id"]
        if book_id not in positive:
            continue
        key = (book_id, row["tag_id"])
        if key in seen:
            raise ValueError("Duplicate (book_id, tag_id) in topic features")
        seen.add(key)
        name, weight = row["tag_name"], row["tag_weight"]
        if (not isinstance(row["tag_id"], int) or row["tag_id"] < 0
                or not isinstance(name, str) or not name.strip()
                or weight is None or not math.isfinite(weight) or not 0 < weight <= 1):
            raise ValueError("Invalid topic feature")
        weights[book_id].append(weight)
        contributions[name][book_id].append((positive[book_id]["rating"] - 3) * weight)
        tag_ids[name].add(row["tag_id"])
    for book_weights in weights.values():
        if not math.isclose(math.fsum(book_weights), 1.0, rel_tol=0, abs_tol=1e-9):
            raise ValueError("Topic weights must sum to one per represented book")

    scores = {name: math.fsum(math.fsum(values) for values in by_book.values())
              for name, by_book in contributions.items()}
    total = math.fsum(scores.values())
    expected = math.fsum(positive[book_id]["rating"] - 3 for book_id in weights)
    if not math.isclose(total, expected, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError("Topic contribution mass does not match covered positive evidence")
    order = sorted(scores, key=lambda name: (-scores[name], -len(contributions[name]), name))
    shares = {name: scores[name] / total for name in order} if total else {}
    topics = []
    for name in order[:top_k]:
        evidence = [dict(_book_details(positive[book_id]),
                         contribution=math.fsum(values))
                    for book_id, values in contributions[name].items()]
        evidence.sort(key=lambda book: (-book["contribution"], -book["rating"], book["book_id"]))
        topics.append({"tag_name": name, "tag_ids": sorted(tag_ids[name]),
                       "preference_score": scores[name], "score_share": shares[name],
                       "supporting_book_count": len(evidence),
                       "top_contributing_books": evidence[:evidence_k]})
    summary = {
        "status": "ok" if total else ("no_high_ratings" if not positive else "no_topic_features"),
        "high_rated_books_with_topics": len(weights),
        "high_rated_books_without_topics": len(positive) - len(weights),
        "coverage": len(weights) / len(positive) if positive else None,
        "topic_count": len(scores), "total_preference_score": total,
    }
    return topics, shares, summary


def build_preferences(user_id, history, topic_features, top_k=10, evidence_k=3):
    """Build a JSON-compatible result from one user's small, materialized history."""
    if not isinstance(user_id, int) or user_id <= 0 or user_id > 2147483647:
        raise ValueError("user_id must be a positive 32-bit integer")
    if top_k < 1 or evidence_k < 1:
        raise ValueError("top_k and evidence_k must be positive")
    history = list(history)
    _validate_history(user_id, history)
    positive = {row["book_id"]: row for row in history if row["rating"] >= 4}
    topics, shares, topic_summary = _topic_preferences(positive, topic_features, top_k, evidence_k)
    representatives = sorted(positive.values(), key=lambda row: (-row["rating"], row["book_id"]))

    authors = defaultdict(list)
    for row in positive.values():
        # Preserve the raw signature, including coauthor combinations.
        if isinstance(row.get("authors"), str) and row["authors"].strip():
            authors[row["authors"]].append(row)
    author_groups = []
    for signature, rows in authors.items():
        rows.sort(key=lambda row: (-row["rating"], row["book_id"]))
        author_groups.append({"authors": signature, "supporting_book_count": len(rows),
                              "average_rating": math.fsum(row["rating"] for row in rows) / len(rows),
                              "books": [_book_details(row) for row in rows]})
    author_groups.sort(key=lambda row: (-row["supporting_book_count"], -row["average_rating"], row["authors"]))
    missing_authors = len(positive) - sum(len(rows) for rows in authors.values())
    missing_metadata = sum(not row.get("metadata_available", True) for row in history)
    messages = []
    if not history:
        messages.append("未找到该用户的评分历史。")
        topic_summary["status"] = "user_not_found"
    elif not positive:
        messages.append("该用户没有评分不低于 4 分的图书，无法生成正向主题或作者偏好。")
    elif not topics:
        messages.append("高分图书没有有效主题特征，无法生成主题偏好。")
    if topic_summary["high_rated_books_without_topics"]:
        messages.append("部分高分图书缺少主题特征；主题得分占比仅基于有主题的图书。")
    if missing_metadata:
        messages.append("部分评分图书缺少图书元数据，仍保留在评分画像中。")
    if missing_authors:
        messages.append("部分高分图书缺少作者署名，不参与作者偏好统计。")
    return {
        "method_version": METHOD_VERSION, "user_id": user_id,
        "status": "ok" if history else "user_not_found", "analysis_basis": ANALYSIS_BASIS,
        "parameters": {"top_k": top_k, "evidence_k": evidence_k, "positive_rating_threshold": 4},
        "profile": {
            "rated_book_count": len(history),
            "average_rating": math.fsum(row["rating"] for row in history) / len(history) if history else None,
            "high_rated_book_count": len(positive),
            "rating_distribution": [{"rating": rating, "count": sum(row["rating"] == rating for row in history)}
                                    for rating in range(1, 6)],
        },
        "representative_high_rated_books": [_book_details(row) for row in representatives[:top_k]],
        "topics": topics, "topic_summary": topic_summary,
        "topic_score_shares": shares,
        "authors": author_groups[:top_k],
        "author_summary": {"author_group_count": len(author_groups), "missing_author_books": missing_authors},
        "data_quality": {"missing_book_metadata": missing_metadata},
        "messages": messages,
    }


def analyze_user_preferences(ratings, books, topics, user_id, top_k=10, evidence_k=3):
    """Filter in Spark first; collect only one user's history and positive-book topics."""
    from pyspark.sql import functions as F

    if not isinstance(user_id, int) or not 1 <= user_id <= 2147483647:
        raise ValueError("user_id must be a positive 32-bit integer")
    if top_k < 1 or evidence_k < 1:
        raise ValueError("top_k and evidence_k must be positive")
    selected = ratings.filter(F.col("user_id") == user_id).select("user_id", "book_id", "rating").cache()
    try:
        metadata = books.select("book_id", "title", "authors", F.lit(True).alias("metadata_available"))
        history = [row.asDict() for row in selected.join(F.broadcast(metadata), "book_id", "left").collect()]
        _validate_history(user_id, history)
        if any(row["rating"] >= 4 for row in history):
            positive_ids = selected.filter(F.col("rating") >= 4).select("book_id")
            features = topics.select("book_id", "tag_id", "tag_name", "tag_weight").join(
                F.broadcast(positive_ids), "book_id", "inner"
            )
            topic_rows = [row.asDict() for row in features.collect()]
        else:
            topic_rows = []
        return build_preferences(user_id, history, topic_rows, top_k, evidence_k)
    finally:
        selected.unpersist()
