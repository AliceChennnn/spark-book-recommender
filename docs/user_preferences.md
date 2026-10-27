# 基于评分历史的用户阅读偏好

评分不代表用户实际读完图书；以下指标只描述已有评分记录，不推断阅读完成、近期兴趣或因果关系。

## 输入与计算

- 读取 `processed/ratings`、`processed/books` 和 `processed/tag_features_v1/topics`，均不修改。
- 第一阶段主题结构为 `book_id, tag_id, tag_name, tag_count, tag_weight`。主题权重在每本书的白名单主题集合内归一化，保留同义标签各自 ID。本阶段不重新筛选或分类标签。
- Spark 先按 user_id 筛选评分，再关联图书；只将该用户的高分 book_id 与主题表关联。仅收集该用户历史及相关主题到 driver，纯函数负责可测试的汇总，不展开全量评分与标签。内存需求随单个用户历史增长，未来超长历史用户应改为分布式聚合。
- 评分画像包含评分图书数量、平均评分、完整 1～5 分分布、高分图书数量。高分定义为 rating >= 4。
- 每条正向主题证据贡献为 `(rating - 3) * tag_weight`。按规范化后的精确 tag_name 求和；同名不同 ID 合并并保留 tag_ids，同义词不自动合并。支持数量按不同图书计数。
- 主题得分占比以全部主题得分之和为分母，先计算再截取 Top-K，仅是证据份额，不是真实兴趣概率。完整主题向量保存在 topic_score_shares 中，可用于跨用户描述性比较。
- 有主题图书的总贡献应等于其 `sum(rating - 3)`；缺主题图书不参与主题分母并报告覆盖率。权重非法、未归一化、重复主题键或重复用户图书记录会报错，防止悄悄改变证据质量。
- 主题按得分降序、支持图书数降序、名称升序排序。贡献图书按贡献降序、评分降序、book_id 升序排列。
- 作者保留 books.authors 的原始署名组合，不按逗号拆分；只统计高分图书，按支持图书数降序、平均分降序、署名升序排列，并保留每组全部支持图书。缺作者图书单独计数。
- 代表性高分图书按评分降序、book_id 升序选择，不将其称为最近阅读或最喜欢的唯一图书。

## 接口与结构化输出

`preferences.build_preferences` 接收单用户的小规模字典记录并返回 JSON 兼容字典；`preferences.analyze_user_preferences` 接收 Spark DataFrame，完成过滤和关联。导入模块不会启动 Spark 或运行任务。

CLI 必须指定 `--user-id`，支持 `--top-k`（默认 10）、`--evidence-k`（每主题贡献图书数，默认 3）、`--hdfs-root`、`--topics-path` 和可选的 `--output-root`。

结果包含 profile、representative_high_rated_books、topics、topic_summary、topic_score_shares、authors、author_summary、data_quality、messages、sources 及方法版本。空用户平均分为 null；空用户、无高分、无主题均有明确状态和提示，不除以零。

标准输出为单个 JSON 对象。指定 output-root 时，在新的 HDFS 目录内写入单行 JSON 的 part 文件并读回比对；目录已存在则拒绝写入。不覆盖原始数据、模型、评价结果或历史分析产物。后续页面可读取 JSON，无需在页面内复现计算公式。

## 验证命令

```bash
python3 -m unittest discover -s tests -p test_preferences.py -v

docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  src/analyze_preferences.py --user-id 660 --top-k 10 \
  --output-root hdfs://master:9000/user/root/bookrec/analysis/user_preferences_v1/user_660
```

另外对 user_id 1000、2000 使用相同命令，各自写入 user_1000、user_2000 新目录。重复运行需选择新的输出目录。

## 局限

主题来自群体书架标签，并非该用户亲自打的标签；主题白名单保守且并非互斥类别，同义词仍会分散证据。通用大类可能占据多数权重。高分阈值对评分习惯敏感，支持书数和份额也受用户历史规模、系列图书与作者重复影响。没有时间信息参与计算，不描述偏好变化。

不同用户的画像或主题向量不同，只能说明该方法对输入历史产生不同摘要，不能证明推荐有效性或个性化质量。比较应查看完整向量、支持图书及覆盖率，后续再用更多用户和独立任务验证。

## 实际验证结果（2026-10-27）

11 项人工样例测试全部通过，覆盖贡献公式、完整分母、支持计数、贡献图书排序、主题并列排序、同名及同义词边界、作者署名、空用户、无高分、主题部分缺失、元数据缺失和非法输入。

三个用户均在现有 Spark 集群上实际运行成功，HDFS JSON 读回与计算结果一致。另独立扫描一次原始 ratings.csv，核验图书数、平均分、各分值数量、高分数和总正向证据；使用原始 books.csv 核验输出作者组的支持图书与平均分，均通过。生产计算仍只展开指定用户的高分图书与主题。

| 用户 | 评分图书 | 平均评分 | 高分图书 | 1/2/3/4/5 分数量 | 主题数 | 总主题得分 |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| 660 | 132 | 4.0530 | 107 | 0 / 1 / 24 / 74 / 33 | 68 | 140 |
| 1000 | 107 | 3.4112 | 53 | 4 / 19 / 31 / 35 / 18 | 68 | 71 |
| 2000 | 148 | 3.9324 | 103 | 0 / 6 / 39 / 62 / 41 | 74 | 144 |

三人的高分图书主题覆盖率均为 100%，未发现缺失图书元数据或高分图书作者署名。完整主题份额和为 1，总主题得分分别等于 4 分图书数量加上两倍的 5 分图书数量。

| 用户 | 头部主题：得分 / 份额 / 支持图书 |
| --- | --- |
| 660 | fiction 26.0711 / 18.62% / 102；fantasy 15.5708 / 11.12% / 61；young-adult 13.4168 / 9.58% / 74；historical-fiction 7.7541 / 5.54% / 35 |
| 1000 | fiction 17.6824 / 24.90% / 50；horror 7.8539 / 11.06% / 18；classics 5.0373 / 7.09% / 40；graphic-novels 4.0618 / 5.72% / 7 |
| 2000 | fiction 29.1187 / 20.22% / 99；non-fiction 13.3245 / 9.25% / 44；young-adult 9.6831 / 6.72% / 58；classics 9.1389 / 6.35% / 55 |

| 用户 | 作者署名组合：支持图书 / 平均分 | 代表性 5 分图书 |
| --- | --- | --- |
| 660 | Rick Riordan：10 / 4.00；Suzanne Collins：8 / 4.25；Alan Bradley：7 / 4.2857 | The Hunger Games；The Fault in Our Stars；Catching Fire |
| 1000 | Stephen King：7 / 4.1429；Toni Morrison：6 / 4.1667；Bryan Lee O'Malley：2 / 5.00 | Middlesex；The Red Tent；The Stand |
| 2000 | Ann Brashares：5 / 4.80；Roald Dahl, Quentin Blake：4 / 4.75；Laura Ingalls Wilder, Garth Williams：4 / 4.25 | The Hunger Games；Harry Potter and the Sorcerer's Stone；To Kill a Mockingbird |

具体贡献图书可在 JSON 中追溯：660 的 fantasy 主要贡献样例包括 Eragon 和 Gregor and the Code of Claw；1000 的 horror 包括 Bag of Bones、The Bachman Books 和 House of Leaves。代表性图书同分时采用 book_id 排序，只是可复现的样例，不表示用户在同分图书之间的真实偏爱次序。

对完整 topic_score_shares 向量进行比较，缺失主题维度补零。Jensen–Shannon 距离采用以 2 为底的对数及平方根，0 表示份额相同，1 表示支持集完全不相交；这些指标仅用于描述摘要差异，不是推荐质量评价。

| 用户对 | 全主题余弦相似度 | Jensen–Shannon 距离 | 移除 fiction 后余弦相似度 |
| --- | ---: | ---: | ---: |
| 660 / 1000 | 0.7851 | 0.4500 | 0.5340 |
| 660 / 2000 | 0.8891 | 0.3361 | 0.7766 |
| 1000 / 2000 | 0.8504 | 0.3906 | 0.6176 |

这些结果表明三人的证据分布存在差异，但共享大类 fiction 明显影响相似度。三个用户不足以得出普遍个性化效果结论。原始得分不可直接比较兴趣强弱，因为其总量受高分图书数影响。

本次还观察到：660 的 Rick Riordan 支持图书中有多个系列分册；2000 的高分作者组包含完整合作署名；non-fiction/nonfiction、young-adult/ya 等同义标签仍分散份额。它们分别反映系列重复证据、署名组合口径和第一阶段未合并同义词的限制，不能解释为独立兴趣维度。

HDFS 输出根目录为 `hdfs://master:9000/user/root/bookrec/analysis/user_preferences_v1`，下含 user_660、user_1000、user_2000 三个独立目录。本地 JSON 和运行日志保存在 `outputs/user_preferences_v1/`，完整向量比较结果保存在 comparison.json。该 outputs 目录按现有规则被 Git 忽略。

下一步可先审阅标签同义词映射和通用大类处理规则，再用更多评分规模、评分习惯不同的用户检查稳定性；作者组合与系列证据继续保留可追溯图书。以上为后续建议，本阶段未调整主题特征、推荐算法或展示页面。
