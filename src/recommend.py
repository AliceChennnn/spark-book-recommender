
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALSModel


HDFS_ROOT = "hdfs://master:9000/user/root/bookrec"
MODEL_PATH = f"{HDFS_ROOT}/models/als_full_v1"


def recommend(spark, user_id, top_k):
    """为指定用户生成尚未评分图书的 Top-K 推荐。"""

    # 1. 加载训练好的 ALS 模型
    model = ALSModel.load(MODEL_PATH)
    model.setColdStartStrategy("drop")

    # 检查用户是否在模型训练范围内
    known_user = (
        model.userFactors
        .filter(F.col("id") == user_id)
        .limit(1)
        .count()
    )

    if known_user == 0:
        print(f"User {user_id} is unknown to the model.")
        return

    # 2. 读取图书信息与完整评分记录
    books = spark.read.parquet(
        f"{HDFS_ROOT}/processed/books"
    ).select("book_id", "title", "authors")

    ratings = spark.read.parquet(
        f"{HDFS_ROOT}/processed/ratings"
    )

    # 3. 获取该用户已经评分的图书
    rated_books = (
        ratings
        .filter(F.col("user_id") == user_id)
        .select("book_id")
        .distinct()
    )

    # 4. 排除已经评分的图书
    candidates = (
        books
        .join(rated_books, on="book_id", how="left_anti")
        .withColumn("user_id", F.lit(user_id).cast("int"))
    )

    # 5. 预测用户对候选图书的评分
    predictions = (
        model.transform(candidates)
        .filter(
            F.col("prediction").isNotNull()
            & ~F.isnan("prediction")
        )
    )

    # 6. 按预测评分从高到低排序
    top_books = (
        predictions
        .orderBy(
            F.desc("prediction"),
            F.asc("book_id")
        )
        .limit(top_k)
    )

    # 验证推荐结果是否包含用户已评分图书
    overlap = (
        top_books.select("book_id")
        .join(rated_books, on="book_id", how="inner")
        .count()
    )

    print("Already rated books in recommendations:", overlap)
    
    # 7. 输出推荐结果
    print(f"\n===== Recommendations for User {user_id} =====")

    top_books.select(
        "book_id",
        "title",
        "authors",
        F.round("prediction", 3).alias("predicted_rating")
    ).show(top_k, truncate=55)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--top-k", type=int, default=10)

    args = parser.parse_args()

    if args.top_k < 1:
        parser.error("--top-k must be positive")

    spark = (
        SparkSession.builder
        .appName("BookRec-Recommendation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        recommend(spark, args.user_id, args.top_k)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
