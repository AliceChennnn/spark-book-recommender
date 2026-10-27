
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType
)

# HDFS 数据目录
HDFS_ROOT = "hdfs://master:9000/user/root/bookrec/raw"

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("BookRec-DataInspection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. 定义评分数据的 Schema
rating_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("book_id", IntegerType(), True),
    StructField("rating", IntegerType(), True)
])

# 2. 从 HDFS 读取 CSV
ratings = spark.read \
    .option("header", True) \
    .schema(rating_schema) \
    .csv(f"{HDFS_ROOT}/ratings.csv")

books = spark.read \
    .option("header", True) \
    .option("escape", '"') \
    .csv(f"{HDFS_ROOT}/books.csv")

# 3. 检查数据结构
print("\n===== Ratings Schema =====")
ratings.printSchema()

print("\n===== Books Schema =====")
books.printSchema()

# 4. 统计数据规模和评分范围
print("\n===== Ratings Statistics =====")
ratings.agg(
    F.count("*").alias("total_ratings"),
    F.countDistinct("user_id").alias("total_users"),
    F.countDistinct("book_id").alias("rated_books"),
    F.min("rating").alias("min_rating"),
    F.max("rating").alias("max_rating")
).show(truncate=False)

print("\n===== Books Count =====")
print("Total books:", books.count())

# 5. 统计不同评分的数量
print("\n===== Rating Distribution =====")
ratings.groupBy("rating") \
    .count() \
    .orderBy("rating") \
    .show()

# 6. 展示部分图书
print("\n===== Sample Books =====")
books.select(
    "book_id", "title", "authors"
).show(5, truncate=60)

# 7. 检查评分数据中的空值和非法评分
print("\n===== Data Quality =====")

ratings.agg(
    F.sum(
        F.when(F.col("user_id").isNull(), 1).otherwise(0)
    ).alias("null_user_id"),
    F.sum(
        F.when(F.col("book_id").isNull(), 1).otherwise(0)
    ).alias("null_book_id"),
    F.sum(
        F.when(F.col("rating").isNull(), 1).otherwise(0)
    ).alias("null_rating"),
    F.sum(
        F.when(
            (F.col("rating") < 1) |
            (F.col("rating") > 5), 1
        ).otherwise(0)
    ).alias("invalid_rating")
).show()

# 8. 检查重复的用户-图书评分
print("\n===== Duplicate Ratings =====")

duplicates = ratings \
    .groupBy("user_id", "book_id") \
    .count() \
    .filter(F.col("count") > 1)

duplicates.agg(
    F.count("*").alias("duplicate_pairs"),
    F.coalesce(
        F.sum(F.col("count") - 1), F.lit(0)
    ).alias("extra_records")
).show()

# 9. 检查评分记录中的图书能否关联到图书表
print("\n===== Missing Book References =====")

book_ids = books.select(
    F.col("book_id").cast("int").alias("book_id")
).distinct()

missing_books = ratings.select("book_id").distinct() \
    .join(book_ids, on="book_id", how="left_anti")

print("Unmatched book IDs:", missing_books.count())

# 10. 检查图书表中的重复 ID
print("\n===== Duplicate Book IDs =====")

books.groupBy("book_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

spark.stop()
