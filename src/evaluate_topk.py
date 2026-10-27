
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALSModel

ROOT = "hdfs://master:9000/user/root/bookrec"
TOP_K = 10
SAMPLE_USERS = 100

spark = (
    SparkSession.builder
    .appName("BookRec-TopK-Evaluation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    ratings = spark.read.parquet(
        f"{ROOT}/processed/ratings"
    )

    model = ALSModel.load(
        f"{ROOT}/models/als_full_v1"
    )
    model.setColdStartStrategy("drop")

    # 与正式训练时完全相同的测试集划分
    test_condition = (
        (F.pmod(F.col("user_id"), F.lit(20)) != 0)
        & (
            F.pmod(
                F.xxhash64("user_id", "book_id"),
                F.lit(10)
            ) == 0
        )
    )

    train = ratings.filter(~test_condition)
    test = ratings.filter(test_condition)

    # 测试集中评分 >= 4 的图书视为正样本
    positives = test.filter(F.col("rating") >= 4)

    # 从训练集获取模型已知用户，避免引用 ALS 内部特征表
    known_users = train.select("user_id").distinct()

    # 确定性选取 100 名有正样本的已知用户
    users = (
        positives.join(known_users, "user_id")
        .groupBy("user_id")
        .agg(F.count("*").alias("positive_count"))
        .orderBy(F.xxhash64("user_id"))
        .limit(SAMPLE_USERS)
        .cache()
    )

    user_count = users.count()
    if user_count == 0:
        raise RuntimeError("No eligible test users")

    print("Evaluation users:", user_count)

    # 从图书表获取候选 ID，由 ALS 负责过滤未知图书
    items = (
        spark.read.parquet(f"{ROOT}/processed/books")
        .select("book_id")
        .distinct()
    )

    # 构造测试用户与候选图书组合
    candidates = (
        F.broadcast(users.select("user_id"))
        .crossJoin(items)
    )

    # 离线评价只排除训练集中的已评分图书
    seen = (
        train.join(users.select("user_id"), "user_id")
        .select("user_id", "book_id")
    )

    candidates = candidates.join(
        seen,
        ["user_id", "book_id"],
        "left_anti"
    )

    # 预测评分并获取每位用户的 Top 10
    predictions = model.transform(candidates)

    window = Window.partitionBy("user_id").orderBy(
        F.desc("prediction"),
        F.asc("book_id")
    )

    topk = (
        predictions
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= TOP_K)
        .select("user_id", "book_id")
    )

    # 命中的正样本
    hits = (
        topk.join(
            positives.select("user_id", "book_id"),
            ["user_id", "book_id"],
            "inner"
        )
        .groupBy("user_id")
        .agg(F.count("*").alias("hit_count"))
    )

    # 计算每个用户的 Recall@10 和 HitRate@10
    metrics = (
        users.join(hits, "user_id", "left")
        .fillna({"hit_count": 0})
        .withColumn(
            "recall",
            F.col("hit_count") / F.col("positive_count")
        )
        .withColumn(
            "hit",
            F.when(F.col("hit_count") > 0, 1).otherwise(0)
        )
    )

    print("\n===== Top-K Evaluation =====")

    metrics.agg(
        F.count("*").alias("users"),
        F.avg("recall").alias("Recall@10"),
        F.avg("hit").alias("HitRate@10"),
        F.sum("hit_count").alias("total_hits")
    ).show(truncate=False)


    # Popularity Baseline：根据训练集评分次数推荐
    popularity = (
        train.groupBy("book_id")
        .agg(F.count("*").alias("popularity"))
    )

    # 沿用与 ALS 完全相同的候选集合
    popular_candidates = (
        candidates
        .join(F.broadcast(popularity), "book_id", "left")
        .fillna({"popularity": 0})
    )

    popular_window = Window.partitionBy("user_id").orderBy(
        F.desc("popularity"),
        F.asc("book_id")
    )

    popular_topk = (
        popular_candidates
        .withColumn(
            "rank",
            F.row_number().over(popular_window)
        )
        .filter(F.col("rank") <= TOP_K)
        .select("user_id", "book_id")
    )

    popular_hits = (
        popular_topk
        .join(
            positives.select("user_id", "book_id"),
            ["user_id", "book_id"],
            "inner"
        )
        .groupBy("user_id")
        .agg(F.count("*").alias("hit_count"))
    )

    popular_metrics = (
        users
        .join(popular_hits, "user_id", "left")
        .fillna({"hit_count": 0})
        .withColumn(
            "recall",
            F.col("hit_count") / F.col("positive_count")
        )
        .withColumn(
            "hit",
            F.when(F.col("hit_count") > 0, 1).otherwise(0)
        )
    )

    print("\n===== Popularity Baseline =====")

    popular_metrics.agg(
        F.count("*").alias("users"),
        F.avg("recall").alias("Recall@10"),
        F.avg("hit").alias("HitRate@10"),
        F.sum("hit_count").alias("total_hits")
    ).show(truncate=False)


    users.unpersist()

finally:
    spark.stop()
