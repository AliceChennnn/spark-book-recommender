"""Prepare ratings/books Parquet; existing outputs require --overwrite."""

import argparse

from output_safety import add_overwrite_argument, ensure_outputs_available, write_parquet


HDFS_ROOT = "hdfs://master:9000/user/root/bookrec"
RATINGS_PATH = f"{HDFS_ROOT}/processed/ratings"
BOOKS_PATH = f"{HDFS_ROOT}/processed/books"


def run(spark, overwrite=False):
    # Check both destinations before reading data or writing either Parquet.
    ensure_outputs_available(spark, [RATINGS_PATH, BOOKS_PATH], overwrite=overwrite)

    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, IntegerType

    # 1. 读取评分数据
    rating_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("book_id", IntegerType(), True),
        StructField("rating", IntegerType(), True)
    ])

    ratings = spark.read \
        .option("header", True) \
        .schema(rating_schema) \
        .csv(f"{HDFS_ROOT}/raw/ratings.csv")

    # 2. 读取图书数据
    books = spark.read \
        .option("header", True) \
        .option("escape", '"') \
        .csv(f"{HDFS_ROOT}/raw/books.csv")

    # 3. 规范化评分数据
    # 前期质量检查已确认无空值和重复评分
    clean_ratings = ratings.select(
        "user_id", "book_id", "rating"
    )

    # 4. 规范化图书数据
    # 保留其他原始字段，以供后续扩展使用
    clean_books = books \
        .withColumn("book_id", F.col("book_id").cast("int")) \
        .withColumn("average_rating", F.col("average_rating").cast("double")) \
        .withColumn("ratings_count", F.col("ratings_count").cast("long")) \
        .withColumn(
            "original_publication_year",
            F.col("original_publication_year").cast("double")
        ) \
        .withColumn("title", F.trim(F.col("title"))) \
        .withColumn("authors", F.trim(F.col("authors")))

    # 5. 检查图书 ID 的类型转换结果
    invalid_ids = clean_books.filter(
        F.col("book_id").isNull()
    ).count()

    if invalid_ids != 0:
        raise ValueError(f"Invalid book IDs: {invalid_ids}")

    # 6. 保存为 Parquet；仅 --overwrite 显式授权替换两个目标，不修改 raw 数据
    write_parquet(clean_ratings, RATINGS_PATH, overwrite=overwrite)
    write_parquet(clean_books, BOOKS_PATH, overwrite=overwrite)

    print("===== Data Preparation Completed =====")
    print("Ratings:", clean_ratings.count())
    print("Books:", clean_books.count())


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    add_overwrite_argument(parser)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("BookRec-DataPreparation").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        return run(spark, overwrite=args.overwrite)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
