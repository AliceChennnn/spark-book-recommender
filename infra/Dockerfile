# 基于老师的 Spark 3.5.5 镜像
FROM bigdata/spark:3.5.5-hadoop3.3.6

USER root

# 安装 PySpark MLlib 所需 Python 依赖
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends \
        python3-numpy \
        python3-scipy \
        python3-pandas \
        python3-setuptools && \
    rm -rf /var/lib/apt/lists/*

# 扩大 Worker 可用计算内存
RUN sed -i \
    's/^export SPARK_WORKER_MEMORY=.*/export SPARK_WORKER_MEMORY=1g/' \
    /usr/local/spark/conf/spark-env.sh

# 统一 Python 解释器
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
