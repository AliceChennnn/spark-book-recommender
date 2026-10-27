
from pyspark.sql import SparkSession

HDFS_ROOT = "hdfs://master:9000/user/root/bookrec"

spark = SparkSession.builder \
    .appName("BookRec-DataAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. 读取预处理后的 Parquet 数据
ratings = spark.read.parquet(
    f"{HDFS_ROOT}/processed/ratings"
)
books = spark.read.parquet(
    f"{HDFS_ROOT}/processed/books"
)

# 注册临时视图，供 Spark SQL 查询
ratings.createOrReplaceTempView("ratings")
books.createOrReplaceTempView("books")


# 2. 评分分布
rating_distribution = spark.sql("""
    SELECT
        rating,
        COUNT(*) AS rating_count
    FROM ratings
    GROUP BY rating
    ORDER BY rating
""")

print("\n===== Rating Distribution =====")
rating_distribution.show()


# 3. 最热门的 10 本图书
# 热度依据本数据集中的评分次数
popular_books = spark.sql("""
    SELECT
        b.book_id,
        b.title,
        b.authors,
        COUNT(*) AS rating_count,
        ROUND(AVG(r.rating), 3) AS avg_rating
    FROM ratings r
    JOIN books b ON r.book_id = b.book_id
    GROUP BY b.book_id, b.title, b.authors
    ORDER BY rating_count DESC, avg_rating DESC
    LIMIT 10
""")

print("\n===== Most Popular Books =====")
popular_books.show(truncate=50)


# 4. 高评分图书排行榜
# 设置最低评分数量，减少少量评分造成的偏差
high_rated_books = spark.sql("""
    SELECT
        b.book_id,
        b.title,
        COUNT(*) AS rating_count,
        ROUND(AVG(r.rating), 3) AS avg_rating
    FROM ratings r
    JOIN books b ON r.book_id = b.book_id
    GROUP BY b.book_id, b.title
    HAVING COUNT(*) >= 100
    ORDER BY avg_rating DESC, rating_count DESC
    LIMIT 10
""")

print("\n===== Highest Rated Books =====")
high_rated_books.show(truncate=50)


# 5. 用户评分活跃度
user_activity = spark.sql("""
    WITH activity AS (
        SELECT
            user_id,
            COUNT(*) AS rating_count
        FROM ratings
        GROUP BY user_id
    )
    SELECT
        CASE
            WHEN rating_count < 20 THEN '01-19'
            WHEN rating_count < 50 THEN '20-49'
            WHEN rating_count < 100 THEN '50-99'
            ELSE '100+'
        END AS activity_group,
        COUNT(*) AS user_count
    FROM activity
    GROUP BY
        CASE
            WHEN rating_count < 20 THEN '01-19'
            WHEN rating_count < 50 THEN '20-49'
            WHEN rating_count < 100 THEN '50-99'
            ELSE '100+'
        END
    ORDER BY activity_group
""")

print("\n===== User Activity =====")
user_activity.show()


# 6. 保存统计结果，供后续可视化使用
results = {
    "rating_distribution": rating_distribution,
    "popular_books": popular_books,
    "high_rated_books": high_rated_books,
    "user_activity": user_activity,
}

for name, df in results.items():
    output_path = f"{HDFS_ROOT}/analysis/{name}"

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    print(f"Saved: {output_path}")

spark.stop()
print("\n===== Analysis Completed =====")
