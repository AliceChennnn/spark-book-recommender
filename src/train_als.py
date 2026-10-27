
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

HDFS_ROOT = "hdfs://master:9000/user/root/bookrec"
MODEL_PATH = f"{HDFS_ROOT}/models/als_pilot"

spark = SparkSession.builder \
    .appName("BookRec-ALS-Pilot") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    # 1. 读取预处理后的评分数据
    ratings = spark.read.parquet(
        f"{HDFS_ROOT}/processed/ratings"
    )

    # 2. 首次训练只选择约 5% 的用户
    # 保留被选中用户的全部评分记录
    sample = ratings.filter(
        F.pmod(F.col("user_id"), F.lit(20)) == 0
    ).cache()

    print("\n===== Dataset =====")
    print("Sample ratings:", sample.count())
    print("Sample users:",
          sample.select("user_id").distinct().count())

    # 3. 划分训练集、验证集和测试集
    train, validation, test = sample.randomSplit(
        [0.8, 0.1, 0.1],
        seed=42
    )

    print("\n===== Data Split =====")
    print("Train:", train.count())
    print("Validation:", validation.count())
    print("Test:", test.count())

    # 4. 构建 ALS 协同过滤模型
    als = ALS(
        userCol="user_id",
        itemCol="book_id",
        ratingCol="rating",
        rank=10,
        maxIter=5,
        regParam=0.1,
        implicitPrefs=False,
        coldStartStrategy="drop",
        numUserBlocks=3,
        numItemBlocks=3,
        seed=42
    )

    print("\n===== Training ALS =====")
    model = als.fit(train)

    # 5. 在验证集上进行预测
    predictions = model.transform(validation).cache()

    total_validation = validation.count()
    valid_predictions = predictions.count()

    if valid_predictions == 0:
        raise RuntimeError("No valid predictions generated")

    # 6. 计算 RMSE
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )

    rmse = evaluator.evaluate(predictions)

    print("\n===== ALS Evaluation =====")
    print(f"RMSE: {rmse:.4f}")
    print(f"Prediction coverage: "
          f"{valid_predictions}/{total_validation}")

    print("\n===== Prediction Samples =====")
    predictions.select(
        "user_id", "book_id", "rating", "prediction"
    ).show(10, truncate=False)


    # 7. 图书平均评分基线
    # 仅使用训练集计算平均分，避免数据泄漏
    global_avg = train.agg(
        F.avg("rating").alias("avg")
    ).first()["avg"]

    book_avg = train.groupBy("book_id").agg(
        F.avg("rating").alias("book_avg")
    )

    # 使用 ALS 能预测的验证记录进行公平比较
    baseline_predictions = (
        predictions
        .select("user_id", "book_id", "rating")
        .join(book_avg, on="book_id", how="left")
        .withColumn(
            "prediction",
            F.coalesce(F.col("book_avg"), F.lit(global_avg))
        )
    )

    baseline_rmse = evaluator.evaluate(baseline_predictions)

    print("\n===== Model Comparison =====")
    print(f"Item Mean RMSE: {baseline_rmse:.4f}")
    print(f"ALS RMSE:       {rmse:.4f}")
    print(f"RMSE reduction: {baseline_rmse - rmse:.4f}")

    # 8. 保存模型至 HDFS
    model.write().overwrite().save(MODEL_PATH)
    print(f"\nModel saved: {MODEL_PATH}")

    predictions.unpersist()
    sample.unpersist()

finally:
    spark.stop()
