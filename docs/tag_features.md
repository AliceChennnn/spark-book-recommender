# Goodbooks-10k 标签特征

`src/prepare_tags.py` 只读取原始标签 CSV 和已有的 `processed/books`，不读取评分、不训练模型。导入模块不会创建 SparkSession 或执行作业。

## 处理规则

- `goodreads_book_id` 转为 long；`book_id`、`tag_id` 使用 int。`tag_id = 0` 合法。
- CSV 使用显式 schema 和 FAILFAST；无法解析为指定整数类型或超出类型范围的值直接失败。空输入，`goodreads_book_id` / `tag_id` / `count` 任一空值，`goodreads_book_id <= 0` 或 `tag_id < 0` 都是必须修复的异常，在过滤、去重和读取映射前报错，不再静默丢弃。
- 只有 ID 合法且字段非空的 `count <= 0` 记录继续按原规则过滤；`nonpositive_rows` 等审计字段保持原口径。异常计数可重叠，例如无效 ID 且 count 为 0 时仍必须报错，不能由非正计数过滤掩盖。错误消息包含输入计数，调用方传入的审计字典也保留计数。
- 对有效记录按 `(goodreads_book_id, tag_id)` 取最大 count。count 是图书书架累计计数，求和会重复累计；最大值策略也不依赖输入顺序。
- 通过 `books.goodreads_book_id = book_tags.goodreads_book_id` 获取内部 `book_id`，再按 `tag_id` 关联字典。不会按内部 book_id 连接标签表。
- 图书映射必须双向唯一。标签字典完全重复记录可以去重，同一 tag_id 对应多个规范化名字则失败。空字典项、无效映射和关联失败都会阻止输出。
- 标签名去除首尾空白、转小写，将空白和下划线转为连字符，不合并不同 tag_id。

## 两套特征

`general/` 保留所有有效标签，包括阅读状态、收藏和格式等书架标签，用于通用统计，不能直接当作图书类别。

`topics/` 使用代码中的显式白名单 `TOPIC_TAGS`，保留题材、主题、读者年龄或内容形式等兴趣线索。它不是完整或互斥的类别体系；白名单外的标签只是未被本版本选用，不能据此判定为非主题。`to-read`、`currently-reading`、`owned`、`favorites` 等不进入此集合。

同义标签（如 sci-fi / science-fiction）仍保留各自 tag_id，暂不合并语义；后续如需合并，应新增显式主题映射，而不是改变 tag_id 的含义。策略版本和完整白名单保存到 audit。

两套 Parquet 均包含 `book_id, tag_id, tag_name, tag_count, tag_weight`。分别在各自集合内按图书计算 `tag_weight = tag_count / sum(tag_count)`，每本有特征图书的权重和为 1。没有白名单标签的图书在 topics 中没有记录，不伪造主题或零权重；audit 报告缺失数量。

## 运行

在项目根目录执行小规模测试（使用现有容器，不安装依赖）：

```bash
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master 'local[2]' tests/test_prepare_tags.py
```

使用现有集群执行实际预处理：

```bash
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 src/prepare_tags.py
```

默认输出根目录为 `hdfs://master:9000/user/root/bookrec/processed/tag_features_v1`，下含 `general/`、`topics/` 和 `audit/`。audit 为单行 JSON 文本目录，可通过 `hdfs dfs -cat .../audit/part-*` 查看。

输出根目录已存在时拒绝运行，所有写入均使用 errorifexists。重跑需通过 `--output-root` 指定新的目录，不自动覆盖或删除历史产物。失败可能留下部分输出，应使用新目录重试。可用 `--hdfs-root` 切换输入数据根目录；默认保持现有 HDFS 地址。

脚本在写入前、重新读取 Parquet 后均检查行数、唯一键、字段有效性和权重和（误差容限 1e-9）。audit 保存输入数量、清洗及去重数量、关联失败数量、覆盖率和读回检查结果。

## 输入加固验证（2026-10-28）

11 项小规模测试全部通过，使用人工 DataFrame 和新建的容器 file:///tmp CSV，不读取已有标签输入或重跑正式标签预处理，也不写 HDFS。测试覆盖空字段、零 / 负 ID、空表、不可解析 / 越界整数、非正计数过滤及审计；异常 CSV 在读取不存在的图书映射前报错，目标目录不产生。原 max 去重、交叉 Goodreads ID 关联和分别归一化测试继续保留。

正常 Goodbooks-10k 的历史空值 / 无效 ID 数均为 0，因此新增门禁不改变其筛选、去重、ID 关联或权重结果；策略版本 `goodbooks-topics-v1` 不变。下述原始预处理结果保留，本次不重新生成或覆盖。

## 历史验证结果（2026-10-27）

5 项小规模 Spark 测试全部通过。上述集群命令已实际执行成功，生成默认输出目录。

| 阶段 | 记录数 |
| --- | ---: |
| 原始 book_tags | 999,912 |
| 过滤非正计数 | 6 |
| 有效正计数记录 | 999,906 |
| 去重删除记录（8 个重复键组） | 8 |
| general 特征 | 999,898 |
| topics 特征 | 114,662 |

空值、无效 ID、无效图书映射、无效标签字典、关联失败均为 0。两套特征均覆盖 10,000 本书，读回行数与唯一键数量一致，无无效输出行；每本书权重和的最大误差分别为 general `4.11e-15`、topics `4.44e-16`，均小于阈值。
