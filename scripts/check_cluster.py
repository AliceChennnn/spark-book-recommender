from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("BookRec-Cluster-Test") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("========== 集群测试 ==========")
print("Master:", spark.sparkContext.master)

# 1. 测试 Spark SQL
df = spark.range(1, 1001)
result = df.agg(F.sum("id")).first()[0]
print("Spark SQL Sum:", result)

# 2. 检查 Python 任务运行在哪些节点
def check_partition(iterator):
    import socket
    values = list(iterator)
    yield (socket.gethostname(), len(values))

rdd = spark.sparkContext.parallelize(range(1000), 24)
results = rdd.mapPartitions(check_partition).collect()

print("Executor hosts:", sorted(set(x[0] for x in results)))
print("Total records:", sum(x[1] for x in results))

spark.stop()
print("========== 测试结束 ==========")
