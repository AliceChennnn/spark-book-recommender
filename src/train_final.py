
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

ROOT = "hdfs://master:9000/user/root/bookrec"

spark = (
    SparkSession.builder
    .appName("BookRec-ALS-Final")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    ratings = spark.read.parquet(
        f"{ROOT}/processed/ratings"
    )

    # 前期调参只涉及 user_id % 20 == 0 的用户
    # 最终测试集排除这些用户的评分
    non_pilot_user = (
        F.pmod(F.col("user_id"), F.lit(20)) != 0
    )

    # 使用确定性哈希划分评分记录
    # 从未参与调参的用户评分中留出约10%
    test_condition = (
        non_pilot_user &
        (
            F.pmod(
                F.xxhash64("user_id", "book_id"),
                F.lit(10)
            ) == 0
        )
    )

    train = ratings.filter(~test_condition)
    test = ratings.filter(test_condition)

    print("\n===== Full Dataset Split =====")
    print("Train:", train.count())
    test_count = test.count()
    print("Test:", test_count)

    # 使用前期验证集选出的参数
    als = ALS(
        userCol="user_id",
        itemCol="book_id",
        ratingCol="rating",
        rank=20,
        maxIter=5,
        regParam=0.1,
        implicitPrefs=False,
        coldStartStrategy="drop",
        numUserBlocks=3,
        numItemBlocks=3,
        seed=42
    )

    print("\n===== Full ALS Training =====")
    model = als.fit(train)

    predictions = model.transform(test).cache()

    predicted_count = predictions.count()
    if predicted_count == 0:
        raise RuntimeError("No valid test predictions")

    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )

    als_rmse = evaluator.evaluate(predictions)

    # 图书平均评分基线，仅从训练集计算
    global_mean = train.agg(
        F.avg("rating")
    ).first()[0]

    book_means = train.groupBy("book_id").agg(
        F.avg("rating").alias("book_mean")
    )

    baseline = (
        predictions
        .select("book_id", "rating")
        .join(book_means, "book_id", "left")
        .withColumn(
            "prediction",
            F.coalesce(
                F.col("book_mean"),
                F.lit(global_mean)
            )
        )
    )

    baseline_rmse = evaluator.evaluate(baseline)

    print("\n===== Final Test Results =====")
    print(f"ALS RMSE: {als_rmse:.4f}")
    print(f"Item Mean RMSE: {baseline_rmse:.4f}")
    print(
        f"Prediction Coverage: "
        f"{predicted_count}/{test_count}"
    )
    print(
        f"Relative Improvement: "
        f"{(baseline_rmse - als_rmse) / baseline_rmse * 100:.2f}%"
    )

    model_path = f"{ROOT}/models/als_full_v1"
    model.write().overwrite().save(model_path)

    print(f"\nModel saved: {model_path}")

    predictions.unpersist()

finally:
    spark.stop()
