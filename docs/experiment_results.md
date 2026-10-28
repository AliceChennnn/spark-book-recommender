# 实验结果与证据口径

封版审计：2026-10-28。所有数值区分历史快照、本轮只读复测和缺失记录。原始模型、Parquet、正式评价脚本均未改动；新证据不冒充历史训练日志。

## 数据规模、质量与 Parquet

| 指标 | 结果 | 范围 / 证据 |
| --- | ---: | --- |
| 评分 / 用户 / 已评分图书 | 5,976,479 / 53,424 / 10,000 | 本轮 inspect_data.py 全量 CSV |
| 图书元数据行数 | 10,000 | 同上 |
| 评分最小 / 最大值 | 1 / 5 | 同上 |
| user_id / book_id / rating 空值 | 全部 0 | 仅评分表关键字段，不代表元数据所有列无空值 |
| 非法评分 | 0 | rating 不在 1～5 |
| 重复评分键 / 多余记录 | 0 / 0 | 按 user_id、book_id 分组 |
| 失配评分图书 / 重复图书 ID | 0 / 0 | 评分到 books.book_id；图书 ID 检查 |
| 1 / 2 / 3 / 4 / 5 分数量 | 124195 / 359257 / 1370916 / 2139018 / 1983093 | 与已有统计 CSV 相同 |
| 评分 Parquet 逻辑字节 | 21,062,002 | HDFS du；CSV 为 72,126,826，减少约 70.80% |
| 图书 Parquet 逻辑字节 | 1,623,312 | HDFS du；CSV 为 3,286,659，减少约 50.61% |

本轮输出为 [data-inspection.txt](artifacts/metrics/data-inspection.txt)。数据校验和在 [data-checksums.json](artifacts/data-checksums.json)，CSV 统计在 artifacts/statistics。Parquet 由既有 prepare_data.py 生成，本轮未覆盖重跑。大小比较不含 HDFS 副本开销，不是读写速度基准；没有预处理运行时间或加速倍数证据。

## 标签与用户画像（2026-10-27 快照）

| 指标 | 结果 | 证据 |
| --- | ---: | --- |
| 原始图书标签记录 | 999,912 | tag-audit.json |
| 过滤 count<=0 | 6 | 先过滤，再以键取最大正 count |
| 重复键组 / 多余记录 | 8 / 8 | goodreads_book_id、tag_id |
| 通用特征 | 999,898 | 含有效书架状态等，不能全部当图书类别 |
| 主题特征 | 114,662 | 显式主题白名单；两套分别归一化 |
| 两套图书覆盖 | 各 10,000 | 未覆盖书数均 0 |
| 空值、无效 ID、失配引用 | 0 | 审计已列出的字段与关联范围 |
| 通用 / 主题权重和最大误差 | 4.11e-15 / 4.44e-16 | 均小于 1e-9 |

标签关联必须先 books.goodreads_book_id = book_tags.goodreads_book_id，再按 tag_id 关联字典。评分使用的内部 book_id 不可直接与 Goodreads ID 连接。来源为 src/prepare_tags.py，证据为 [tag-audit.json](artifacts/metrics/tag-audit.json)。

| 用户 | 评分图书 | 平均分 | ≥4 分图书 | 主题覆盖 | 总主题得分 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 660 | 132 | 4.053030 | 107 | 100% | 140 |
| 1000 | 107 | 3.411215 | 53 | 100% | 71 |
| 2000 | 148 | 3.932432 | 103 | 100% | 144 |

贡献定义为 `(rating-3)*tag_weight`，仅高分书参与。占比是贡献份额，不是真实兴趣概率；作者保留原始合作署名。对应 src/preferences.py、src/analyze_preferences.py，全部画像在 artifacts/demo/preferences，完整向量比较在 artifacts/metrics/preferences-comparison.json。画像与作者结果已在前阶段独立扫描原始 CSV 核验，本轮 11 项逻辑测试再次通过。

## ALS 参数与历史记录边界

tune_als.py 的参数网格为：

| rank | maxIter | regParam | 当时验证 RMSE |
| ---: | ---: | ---: | --- |
| 10 | 5 | 0.1 | 未找到原始日志 |
| 20 | 5 | 0.1 | 未找到原始日志 |
| 10 | 10 | 0.1 | 未找到原始日志 |
| 10 | 5 | 0.05 | 未找到原始日志 |

调参采用 user_id%20==0 的约 5% 用户，80/10/10 randomSplit(seed=42)。train_final.py 的已选配置为 rank=20、maxIter=5、regParam=0.1、显式评分、seed=42、3×3 blocks；保存模型元数据能直接证实 rank=20 和 Spark 3.5.5，但不能单靠 ALSModel 元数据证实 regParam / maxIter。缺少当时每组分数，不能独立复核优选过程或绘制调参收益曲线。封版期间不重训来补造“历史结果”。

## 冻结模型 RMSE 只读复测（2026-10-28）

正式测试条件为：

```text
pmod(user_id,20)!=0 AND pmod(xxhash64(user_id,book_id),10)=0
```

哈希输入顺序和 INT 类型与 train_final.py 相同。训练 5,408,526 条、测试 567,953 条；加载已有 als_full_v1，不调用 fit。有效预测 567,953 条，覆盖率 100%。

| 方法 | RMSE |
| --- | ---: |
| ALS 冻结模型 | 0.8360104944 |
| 训练集图书平均评分基线 | 0.9533267881 |

相对 RMSE 降低 `(baseline-ALS)/baseline` 为 12.305989%，约 12.31%。图书均分只从训练集计算，在 ALS 能预测的同一测试记录上比较，缺图书均分才回退训练全局均分。这是评分预测基线，不能称为 Popularity 排序基线的 RMSE。

证据：[frozen-model-rmse.json](artifacts/metrics/frozen-model-rmse.json)，脚本 scripts/audit_frozen_model.py，保存了原 train_final.py 的 SHA-256。图表：[rmse_comparison.png](artifacts/figures/rmse_comparison.png)。这是本轮复测，不是找回的历史训练控制台输出。

## Top-K 只读复测（2026-10-28）

直接运行未修改的 src/evaluate_topk.py，加载同一冻结模型。测试评分 ≥4 为正样本；在有正样本、训练集中已知的非试验用户中，以 xxhash64(user_id) 排序选 100 人。每人从图书表候选中只排除训练已评分图书，保留测试正样本供命中评价。Recall 是逐用户命中数 / 正样本数的宏平均，HitRate 是至少命中一次的用户占比。

| 方法 | Recall@10 | HitRate@10 | 总命中 |
| --- | ---: | ---: | ---: |
| ALS | 0.0091422466 | 0.08 | 8 |
| Popularity | 0.0660770757 | 0.32 | 50 |

Popularity 仅从训练集计数，同分按 book_id；没有使用 books.ratings_count 或测试评分。图：[topk_comparison.png](artifacts/figures/topk_comparison.png)。结构化值在 [topk.json](artifacts/metrics/topk.json)，原 stdout 在 [topk.stdout.txt](artifacts/metrics/topk.stdout.txt)。这是单一 100 人确定性样本，无置信区间或全用户结果，不构成模型全面排名。

展示推荐排除全部历史，包括测试记录；离线评价只排除训练历史。不能用展示候选直接重算这些指标，否则会把测试正样本预先删除。

## 推荐与 WebUI 验收范围

2026-10-27：660、1000、2000、999999、661 各请求两模式，共 10 次，各返回 10 本，已评分重合均 0。661 的 10 条保留测试记录也排除；999999 的 ALS 明确回退至 Popularity，结果与显式热门模式相同。已知用户两模式 Top-10 重合为 0，只说明输出不同，不证明质量更好。原 ALS CLI 排序 / 分数兼容性通过。证据：artifacts/metrics/recommendations-acceptance.json。

WebUI 的本机 / 局域网 HTTP、三个演示画像、双模式、回退、图片及 JSON 下载均通过；Windows 访问由用户于 2026-10-27 确认。记录于 artifacts/metrics/webui-validation.json 和 webui-cache-validation.json。首次观察：

| 请求 | 秒 | 测量范围 |
| --- | ---: | --- |
| 662 偏好 | 26.297 | 首次容器计算并立即重复一次缓存读取 |
| 662 ALS | 27.803 | 同上，包含 Spark 启动 |
| 662 Popularity | 26.034 | 同上 |
| 999999 空画像 | 20.884 | 同上，返回 user_not_found |

每组首次启动 1 个 Spark 应用，重复和新 Python 进程请求均增加 0 个作业。没有缓存延迟基准、并发吞吐、P95 或稳定性压测，不将这些单次观察写成平均性能。

本轮 2026-10-28：标签 5、偏好 11、推荐 11、页面 / 后端 14 项均通过，总计 41；pip check、shell 语法、Compose 合并检查通过。集群 Sum=500500、RDD=1000，完整记录见 artifacts/metrics/cluster-check.txt、tests-summary.json。未进行虚拟机重启 / 卷恢复演练，不声称所有环境都能一键复现。
