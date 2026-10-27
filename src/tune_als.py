
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

HDFS_ROOT = "hdfs://master:9000/user/root/bookrec"

spark = SparkSession.builder \
    .appName("BookRec-ALS-Tuning") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 与首次实验使用相同的抽样及划分策略
ratings = spark.read.parquet(
    f"{HDFS_ROOT}/processed/ratings"
)

sample = ratings.filter(
    F.pmod(F.col("user_id"), F.lit(20)) == 0
).cache()
sample.count()

train, validation, test = sample.randomSplit(
    [0.8, 0.1, 0.1], seed=42
)

# 固定训练集和验证集，保证参数比较公平
train = train.cache()
validation = validation.cache()

print("Train:", train.count())
print("Validation:", validation.count())

params = [
    (10, 5, 0.1),
    (20, 5, 0.1),
    (10, 10, 0.1),
    (10, 5, 0.05),
]

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

results = []

for rank, max_iter, reg_param in params:
    print(f"\nTraining: rank={rank}, "
          f"maxIter={max_iter}, regParam={reg_param}")

    als = ALS(
        userCol="user_id",
        itemCol="book_id",
        ratingCol="rating",
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        implicitPrefs=False,
        coldStartStrategy="drop",
        numUserBlocks=3,
        numItemBlocks=3,
        seed=42
    )

    model = als.fit(train)

    predictions = model.transform(validation).cache()
    predicted_count = predictions.count()

    rmse = evaluator.evaluate(predictions)

    coverage = predicted_count / validation.count()

    results.append((
        rank, max_iter, reg_param, rmse, coverage
    ))

    print(f"RMSE: {rmse:.4f}")
    print(f"Coverage: {coverage:.4%}")

    predictions.unpersist()

print("\n===== Tuning Results =====")

for row in results:
    print(row)

best = min(results, key=lambda x: x[3])

print("\n===== Best Parameters =====")
print(f"rank={best[0]}")
print(f"maxIter={best[1]}")
print(f"regParam={best[2]}")
print(f"Validation RMSE={best[3]:.4f}")

train.unpersist()
validation.unpersist()
sample.unpersist()
spark.stop()
