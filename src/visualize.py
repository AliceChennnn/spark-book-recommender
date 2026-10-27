
import csv
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "outputs" / "analysis"
FIG_DIR = ROOT / "outputs" / "figures"

FIG_DIR.mkdir(parents=True, exist_ok=True)


def load_csv(name):
    """读取 Spark 导出的统计结果。"""
    with open(DATA_DIR / f"{name}.csv",
              encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


# 1. 评分分布
ratings = load_csv("rating_distribution")
ratings.sort(key=lambda x: int(x["rating"]))

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(
    [r["rating"] for r in ratings],
    [int(r["rating_count"]) for r in ratings]
)
ax.set_title("Book Rating Distribution")
ax.set_xlabel("Rating")
ax.set_ylabel("Number of Ratings")
fig.tight_layout()
fig.savefig(FIG_DIR / "rating_distribution.png", dpi=180)
plt.close(fig)


# 2. 热门图书 Top 10
books = load_csv("popular_books")
books.reverse()

# 缩短书名，避免坐标轴过于拥挤
titles = [
    b["title"][:38] + ("..." if len(b["title"]) > 38 else "")
    for b in books
]
counts = [int(b["rating_count"]) for b in books]

fig, ax = plt.subplots(figsize=(11, 6))
ax.barh(titles, counts)
ax.set_title("Top 10 Most Popular Books")
ax.set_xlabel("Number of Ratings")
fig.subplots_adjust(left=0.42, right=0.96)
fig.savefig(FIG_DIR / "popular_books.png", dpi=180)
plt.close(fig)


# 3. 用户活跃度
activity = load_csv("user_activity")
groups = ["01-19", "20-49", "50-99", "100+"]

counts_by_group = {
    row["activity_group"]: int(row["user_count"])
    for row in activity
}

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(
    groups,
    [counts_by_group.get(g, 0) for g in groups]
)
ax.set_title("User Rating Activity")
ax.set_xlabel("Number of Ratings per User")
ax.set_ylabel("Number of Users")
fig.tight_layout()
fig.savefig(FIG_DIR / "user_activity.png", dpi=180)
plt.close(fig)

print(f"Charts saved to: {FIG_DIR}")
