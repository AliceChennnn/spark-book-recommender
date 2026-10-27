# ALS / Popularity 推荐

## 统一接口

`BookRecommender.from_hdfs(spark)` 读取已有评分和图书表，随后调用 `service.recommend(user_id, top_k=10, mode="als")`。同一实例可以重复服务不同用户和模式。模型按需加载一次；Popularity 首次请求时构建并缓存约 10,000 行训练集计数，之后复用。调用 `service.close()` 释放自有缓存，不停止调用方的 SparkSession。

接口返回 JSON 兼容字典，包含 requested_algorithm、actual_algorithm、fallback_reason、status、top_k、returned_count、messages 和 recommendations。每条推荐统一包含：

```text
user_id, book_id, title, authors, rank, score, score_type, algorithm
```

结果仅 collect 最终 Top-K；候选过滤、计数与 ALS 批量预测均在 Spark 中执行，不逐书提交任务。评分和图书 DataFrame 在服务实例内复用，不自动缓存数百万评分记录。服务本身不打印、不写入 HDFS，也不训练模型。

## 算法与数据边界

- ALS 加载 `hdfs://master:9000/user/root/bookrec/models/als_full_v1`，按预测分数降序、book_id 升序推荐。score_type 为 als_predicted_rating；分数不截断到 1～5，也不是概率。
- Popularity 从 `processed/ratings` 复原正式训练集，按每本图书的评分记录数降序、book_id 升序推荐。score_type 为 training_rating_count；统一 score 使用 double，值为整数计数。不会使用 books.ratings_count。
- 正式测试条件与 train_final.py 一致：`pmod(user_id, 20) != 0 AND pmod(xxhash64(user_id, book_id), 10) = 0`。训练集严格为其补集，不重新随机划分。
- 哈希输入顺序与 INT 类型都必须保持不变；把 ID 改成 LONG 会改变 xxhash64 结果，因此热度计算遇到不同 schema 会报错。
- 两种展示推荐均从图书表出发，通过 left_anti 排除用户全部已评分 book_id，包括正式保留测试评分。这里只利用测试记录的 book_id 排除已知历史，不将测试评分用于模型训练或热度统计。
- 原有离线评价仍只排除训练历史。本阶段未修改 train_final.py、evaluate_topk.py 或历史评价结果，展示推荐之间的重合数不作为质量指标。
- 训练中未出现的图书在 Popularity 中计数为 0，同分按 book_id 排序；ALS 对未知图书及非有限预测直接排除。已知用户的 ALS 不补入热门图书，结果不足 K 时返回 partial，完全无可评分候选时返回 empty。

## 冷启动

请求 ALS 时，以模型 userFactors 检查用户是否已知。未知用户自动使用 Popularity，requested_algorithm 保持 als，actual_algorithm 为 popularity，fallback_reason 为 unknown_als_user，同时返回明确提示。显式 Popularity 不加载 ALS 模型，也能服务未知用户。

模型缺失、读取失败和推理异常不会伪装成用户冷启动；这类错误正常抛出。参数必须为正整数 user_id（32 位范围）、正整数 top_k，模式必须为 als 或 popularity。

## CLI 与兼容性

旧命令继续默认 ALS，保留 --user-id、--top-k，默认 Top-K 为 10；三参数 `recommend(spark, user_id, top_k)` 也继续可用，并新增结构化返回值。表格仍提供 predicted_rating 列；新增 rank、请求算法和实际算法显示。未知用户的行为按本阶段要求从退出改为明确回退。

```bash
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  src/recommend.py --user-id 660 --top-k 10

docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  src/recommend.py --user-id 660 --top-k 10 --mode popularity

docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  src/recommend.py --user-id 999999 --top-k 10 --mode als --format json
```

--format json 输出完整结果及回退元数据；默认 table 以中文分别显示请求算法、实际使用算法。

## 验证

```bash
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master 'local[2]' tests/test_recommenders.py

docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  scripts/validate_recommenders.py
```

小数据测试使用固定预测桩，不训练模型。集群验收复用同一服务实例验证 660、1000、2000、999999，并额外验证非试验用户 661，因为前三名用户均能被 20 整除，没有正式保留测试评分。验收检查全历史排除、排序、分数类型、回退一致性、全量图书训练热度计数与独立划分表达式一致性，以及原 ALS 排序和分数兼容性。

验收脚本只在新的本地 `outputs/recommendation_modes_v1/` 保存各用户各模式 JSON 和 summary.json；目录已存在会拒绝重跑，可用 --output-dir 指定新目录。不会写入 HDFS 或改动正式评价。后续 Streamlit 可以直接复用服务和结果结构，自行管理 SparkSession 与服务实例生命周期。

## 实测结果（2026-10-27）

11 项人工小数据 Spark 测试全部通过。集群验收的 5 名用户、共 10 次模式请求均返回 10 条推荐，排序、唯一性、结果字段、分数类型和全部已评分排除检查均通过。

原始评分共 5,976,479 条，按原规则复原训练评分 5,408,526 条、保留测试评分 567,953 条。全部图书的训练热度计数与独立原始划分表达式对比，差异为 0；没有将保留测试评分计入热度。

| 用户 | 全部已评分图书 | 其中保留测试图书 | 每模式返回 | 已评分重合 | 两模式 Top-10 重合 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 660 | 132 | 0 | 10 | 0 | 0 |
| 1000 | 107 | 0 | 10 | 0 | 0 |
| 2000 | 148 | 0 | 10 | 0 | 0 |
| 999999 | 0 | 0 | 10 | 0 | 10（ALS 请求实际已回退） |
| 661 | 106 | 10 | 10 | 0 | 0 |

| 用户 | ALS 前三名（book_id / 预测分数） | Popularity 前三名（book_id / 训练评分次数） |
| --- | --- | --- |
| 660 | 3628 / 4.858585；3491 / 4.831054；6920 / 4.825193 | 2 / 19725；4 / 17280；3 / 15368 |
| 1000 | 3628 / 4.028995；9531 / 4.026910；9842 / 3.966331 | 1 / 20691；2 / 19725；4 / 17280 |
| 2000 | 3753 / 4.489499；422 / 4.489448；9569 / 4.467373 | 3 / 15368；23 / 14185；7 / 14025 |
| 661 | 3628 / 5.013201；8926 / 4.988034；3885 / 4.976361 | 2 / 19725；4 / 17280；3 / 15368 |

例如，660 的 ALS 第一名为 The Complete Calvin and Hobbes，第二名为 Just Mercy；1000 的前三名还包括 Crush 和 Humans of New York: Stories；2000 的前两名为 Harry Potter 的两个合集，第三名为 The Hate U Give。完整书名、作者、排名、未舍入分数均保留在各 JSON 中。

999999 的请求算法为 als，实际算法为 popularity，回退原因为 unknown_als_user；10 条结果与显式 Popularity 完全相同。前三名为 The Hunger Games（20691）、Harry Potter and the Sorcerer's Stone（19725）、To Kill a Mockingbird（17280）。

使用相同已加载模型重现旧 CLI 的候选和排序逻辑，660 的 10 个 book_id 顺序完全一致，分数在 1e-7 容限内一致。另实际执行不带 --mode 的旧命令，成功默认使用 ALS，输出 predicted_rating 列及 10 条结果；首三名为 3628、3491、6920。

本轮观察到 661 的 ALS 分数超过 5、2000 的推荐包含同系列多个合集，均保持原模型预测及按 book_id 去重的原有语义。没有实施评分截断、合集语义去重或多样性重排。两模式 Top-10 不重合仅说明输出不同，不能据此判定哪种算法更优。

## 后续页面调用示例

```python
from recommenders import BookRecommender

service = BookRecommender.from_hdfs(spark)
try:
    result = service.recommend(user_id=660, top_k=10, mode="als")
    rows = result["recommendations"]
finally:
    service.close()
```

实际页面应在可访问现有 Spark/HDFS 的运行环境中维护并复用服务实例；每次请求读取 actual_algorithm、fallback_reason 和 messages，避免把热门回退展示为个性化 ALS。页面只需消费统一结果，不必复制划分或候选过滤逻辑。

## 局限

Popularity 是全局训练热度，用户差异主要来自已评分排除；其分数不可与 ALS 预测分数直接比较。ALS 受旧模型和候选覆盖限制，无法识别训练后新增用户的潜在偏好。热门回退能提供结果，但不能视为个性化推荐。算法可能偏向热门、系列和相似作者，不提供多样性或公平性保证。本轮只验证工程行为，不重新计算推荐质量指标或调参。
