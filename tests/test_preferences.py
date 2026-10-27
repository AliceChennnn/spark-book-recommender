"""Small deterministic tests; the pure summary functions need no Spark runtime."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from preferences import build_preferences  # noqa: E402


def book(book_id, rating, authors="A, B", user_id=1, **extra):
    return dict(user_id=user_id, book_id=book_id, rating=rating,
                title=f"Book {book_id}", authors=authors, **extra)


def feature(book_id, tag_id, name, weight):
    return dict(book_id=book_id, tag_id=tag_id, tag_name=name, tag_weight=weight)


class PreferencesTest(unittest.TestCase):
    def test_contributions_support_and_all_topic_denominator(self):
        result = build_preferences(1, [book(1, 5), book(2, 4), book(3, 3)], [
            feature(1, 0, "fantasy", 0.75), feature(1, 1, "fiction", 0.25),
            feature(2, 0, "fantasy", 1.0), feature(3, 2, "horror", 1.0),
        ], top_k=1, evidence_k=1)
        top = result["topics"][0]
        self.assertEqual(top["tag_name"], "fantasy")
        self.assertAlmostEqual(top["preference_score"], 2.5)
        self.assertAlmostEqual(top["score_share"], 2.5 / 3)
        self.assertEqual(top["supporting_book_count"], 2)
        self.assertEqual(top["top_contributing_books"][0]["book_id"], 1)
        self.assertAlmostEqual(top["top_contributing_books"][0]["contribution"], 1.5)
        self.assertEqual(result["profile"]["rated_book_count"], 3)
        self.assertEqual(result["profile"]["average_rating"], 4)
        self.assertEqual(result["profile"]["high_rated_book_count"], 2)
        self.assertEqual(len(result["topic_score_shares"]), 2)
        self.assertNotIn("horror", result["topic_score_shares"])
        self.assertAlmostEqual(sum(result["topic_score_shares"].values()), 1)

    def test_author_signature_and_representative_ties(self):
        result = build_preferences(1, [book(2, 5), book(1, 5), book(3, 4, "A"), book(4, 2)], [])
        self.assertEqual(result["authors"][0]["authors"], "A, B")
        self.assertEqual(result["authors"][0]["supporting_book_count"], 2)
        self.assertEqual(result["authors"][0]["average_rating"], 5)
        self.assertEqual([b["book_id"] for b in result["authors"][0]["books"]], [1, 2])
        self.assertEqual([b["book_id"] for b in result["representative_high_rated_books"]], [1, 2, 3])

    def test_empty_user_and_json_serialization(self):
        result = build_preferences(1, [], [])
        self.assertEqual(result["status"], "user_not_found")
        self.assertIsNone(result["profile"]["average_rating"])
        self.assertEqual([r["count"] for r in result["profile"]["rating_distribution"]], [0] * 5)
        self.assertEqual(result["topics"], [])
        self.assertEqual(result["authors"], [])
        self.assertTrue(result["messages"])
        json.dumps(result, allow_nan=False)

    def test_no_high_ratings(self):
        result = build_preferences(1, [book(1, 1), book(2, 3)], [feature(1, 0, "fiction", 1)])
        self.assertEqual(result["topic_summary"]["status"], "no_high_ratings")
        self.assertEqual(result["topics"], [])
        self.assertEqual(result["authors"], [])
        self.assertEqual(result["topic_score_shares"], {})

    def test_no_topics_and_partial_coverage(self):
        history = [book(1, 5), book(2, 4)]
        empty = build_preferences(1, history, [])
        self.assertEqual(empty["topic_summary"]["status"], "no_topic_features")
        self.assertEqual(empty["topic_summary"]["coverage"], 0)
        partial = build_preferences(1, history, [feature(1, 0, "fiction", 1)])
        self.assertEqual(partial["topic_summary"]["coverage"], 0.5)
        self.assertEqual(partial["topic_summary"]["total_preference_score"], 2)
        self.assertEqual(partial["topics"][0]["score_share"], 1)
        self.assertTrue(partial["messages"])

    def test_tied_topics_and_input_order_are_deterministic(self):
        history = [book(1, 4)]
        features = [feature(1, 1, "z", 0.5), feature(1, 0, "a", 0.5)]
        result = build_preferences(1, history, features)
        self.assertEqual([r["tag_name"] for r in result["topics"]], ["a", "z"])
        self.assertEqual(result, build_preferences(1, history, list(reversed(features))))

    def test_identical_names_combine_but_aliases_remain_separate(self):
        result = build_preferences(1, [book(1, 4)], [
            feature(1, 0, "sci-fi", 0.25), feature(1, 1, "sci-fi", 0.25),
            feature(1, 2, "science-fiction", 0.5),
        ])
        self.assertEqual(len(result["topics"]), 2)
        by_name = {row["tag_name"]: row for row in result["topics"]}
        self.assertEqual(by_name["sci-fi"]["tag_ids"], [0, 1])
        self.assertEqual(by_name["sci-fi"]["supporting_book_count"], 1)
        self.assertAlmostEqual(by_name["sci-fi"]["preference_score"], 0.5)

    def test_missing_metadata_and_author_are_reported(self):
        result = build_preferences(1, [book(1, 5, None, metadata_available=False)], [])
        self.assertEqual(result["profile"]["rated_book_count"], 1)
        self.assertEqual(result["data_quality"]["missing_book_metadata"], 1)
        self.assertEqual(result["author_summary"]["missing_author_books"], 1)
        self.assertEqual(result["authors"], [])

    def test_duplicate_or_other_user_history_is_rejected(self):
        for history in ([book(1, 4), book(1, 5)], [book(1, 4, user_id=2)]):
            with self.assertRaises(ValueError):
                build_preferences(1, history, [])

    def test_invalid_or_duplicate_features_are_rejected(self):
        for features in ([feature(1, 0, "fiction", 0.5)],
                         [feature(1, 0, "fiction", float("nan"))],
                         [feature(1, 0, "fiction", 1)] * 2):
            with self.assertRaises(ValueError):
                build_preferences(1, [book(1, 4)], features)

    def test_invalid_arguments_and_rating(self):
        for args in ((0, [], []), (1, [], [], 0), (1, [book(1, 6)], [])):
            with self.assertRaises(ValueError):
                build_preferences(*args)


if __name__ == "__main__":
    unittest.main(verbosity=2)
