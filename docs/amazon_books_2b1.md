# 阶段 2B-1：Books 评分获取、审计与工程子集

当前定位：本文件保留已执行的 Amazon 探索及审计证据，不代表本分支须继续完成 Amazon 接入。停止后续 K-Core 抽样、元数据下载、模型训练和评价开发；不删除已有数据或结果。系统默认 Goodbooks-10k，当前范围见 [数据集适配接口说明](dataset_adaptation.md)。下文历史后续步骤不再作为当前计划。

日期：2026-10-28。分支 `feature/dataset-switching`，HEAD `09d18ea7980a6079e067c4f9999a79222423f728`。开始时有上一轮未提交的 amazon_books_2a.md、amazon_books_schema.py、test_amazon_books_schema.py，保留作为本轮基础，没有 stash、丢弃或提交。

## 当前执行边界

已经实现下载器、SQLite 流式审计和确定性抽样，并完成开发阶段的人工 / 首部片段测试。随后用户明确批准唯一评分文件下载及全量审计 / 工程抽样。**Books.csv 已完整下载并验证大小和 SHA-256，后续实测结果另记于本文末尾。** 本轮未下载元数据，不读取 / 写入 HDFS，不运行 ALS、不改 Goodbooks、模型、实验结果或 WebUI。

用户明确要求在批准前不开始完整传输，因此下载命令默认拒绝，只有操作者已获批准后才能传入 `--approved-download`。预检查只查询官方目录 API，绝不调用 CSV 下载。

## 固定来源与预检查

- 仓库：McAuley-Lab/Amazon-Reviews-2023。
- 修订：`37fbebc695620f07a2dc976de770540b74a60ac6`。
- 文件：`benchmark/5core/rating_only/Books.csv`。
- [固定修订页面](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/blob/37fbebc695620f07a2dc976de770540b74a60ac6/benchmark/5core/rating_only/Books.csv)。本轮仅查询目录 API，声明大小 **550,245,256 字节**，LFS SHA-256 与用户给定值一致：`6e65a282b42894099a105f3a30dae09354e6cf423b8b81f0014b21a1fd8d3add`。
- 目标：`data/raw/amazon-books-5core/Books.csv`；预检查时目标、part、part 身份记录及下载锁均不存在。
- curl / wget 可用，hf / aria2c 未安装；下载器用现有 Python urllib，沿用现有 HTTP/HTTPS 代理，不安装依赖、不改代理配置。预检查脱敏输出代理，不打印凭据。
- 项目与 /tmp 同一文件系统，开始时可用约 **20.716 GiB**。建议为源文件、SQLite 索引、排序临时空间及子集至少预留 8 GiB；这是工作预算，非全量运行实测容量保证。

```bash
.venv/bin/python scripts/amazon_books_ratings.py preflight --check-remote
```

## 下载与完整性

`src/amazon_books_download.py` 仅处理这一份已固定的评分来源。失败重试默认 3 次，保留 `.part` 与 `.part.json` 的完整来源身份。已有目标拒绝覆盖；未知 / 来源不一致的 part 不续写。续传必须收到正确的 206 / Content-Range，服务器忽略 Range 时拒绝追加，不截断已有 part。空 part 也可以在身份匹配时恢复。

成功前核对字节数和实际 SHA-256，失败不产生 Books.csv 或成功凭据。经核验后使用同目录 hard link 独占发布并移除 part 名称，不复制完整文件，也不创建 HF 缓存副本。下载锁避免同一目标并发写入。普通失败自动释放锁，进程被强制终止留下的锁 / 损坏 part 需人工审计，不自动删除或覆盖。

以下命令已在收到本次明确批准后执行，目标文件为 550,245,256 字节，实际 SHA-256 与固定来源一致：

```bash
.venv/bin/python scripts/amazon_books_ratings.py download --approved-download
```

此参数不批准元数据下载、HDFS 导入或模型训练。若目标本来存在，应单独核对后审计，不通过重新下载覆盖。

## 审计实现与定义

`src/amazon_books_ratings.py`：先流式核对文件大小与 SHA-256，再扫描 CSV；不将全体记录加载为 Python 对象。Python 批次默认 5,000 行，SQLite 缓存 32 MiB，temp_store=FILE。SQLite 保存来源 ID、pair 计数及度统计，不保存全体评分对象副本。使用独占新输出目录，暂存后发布，最终报告最后移动；中断可能留下不完整输出目录，重跑应先审计状态并使用新路径。

输出 audit_report.json、audit.sqlite、issues.jsonl，**不生成 prepared manifest**：

- 总解析记录（包括空行 / 异常宽度）、评分分布、各类异常计数；严重 CSV / UTF-8 解析错误记录为 scan_complete=false，不把已扫前缀当全量。
- 用户 / 商品 distinct 是四字段可解释记录中的非空 ID；非法评分 / 时间不会被静默从 ID 计数中排除。无法解释列结构的记录单独统计。
- pair 重复组数、额外记录数。raw 交互度按记录计数，unique degree 按唯一 user/item 键计数，二者分别记录，防止重复记录虚增 5-Core。
- 时间戳缺失、格式错误、不可表示值、超出发布方声明时段分别统计；统一 Unix 毫秒，12 / 13 位都可合法。实际 min/max 包含可解释的毫秒值，异常时段单独标记，不猜单位或修正。
- 用户 / 商品交互 min、median、p95、max、低于 5 的节点数及度直方图；median 为中间两个值的平均，p95 为 nearest rank。
- 评分分布通常只有 1～5；极端连续分值情况下 JSON 显示最多 1,000 个值并标明截断，完整分布仍在 SQLite scores 表，避免内存 / 报告膨胀。
- 实际源 SHA、来源版本，SQLite 文件字节与摘要、观测暂存字节、前后可用空间、进程峰值 RSS、用时。暂存观测不保证捕获短暂的 SQLite journal / 排序峰值，磁盘可用量也可能受其他进程影响。

异常仍在原始 CSV 中，issues.jsonl 保存定位和原因，不自动删除、合并或平均。只有完成扫描、无异常且无重复的审计才允许抽样。对源文件的 inode / 大小 / mtime 变化拒绝继续；对审计索引也验证摘要，避免错配或篡改后的索引被复用。

完整评分获批下载并通过摘要后使用以下入口：

```bash
.venv/bin/python scripts/amazon_books_ratings.py audit \
  --output outputs/datasets/amazon-books-5core/full-audit-37fbebc-v1
```

official_full 是默认 scope，强制指定修订、大小与摘要匹配。人工 / 片段必须显式声明 synthetic / prefix，并给出它们自己的摘要和字节数，不能把 64 KiB 片段标成完整文件。

## 工程抽样与输出

固定默认种子 20261028，复用阶段 2A 的用户哈希定义。SQLite 按哈希 / 原始 user_id 排序；选择累计评分落在 [100000,300000] 且最接近 200000 的完整用户前缀，同距离选较小前缀。用户历史不截断；没有合法前缀只写 infeasible 报告，不生成不足量 CSV、不跳过重用户或悄悄删低频商品。

选中用户最多 300,000 个（受最大评分条数约束）；只把这部分 ID 保留在集合中，输出评分也只在独立小 SQLite 中排序。ratings.csv 按原始 user_id / parent_asin 稳定排序，因此输入行顺序不同，成员及子集 CSV 摘要仍一致。保留原始字符串 ID、评分文本及毫秒 timestamp；不改为内部 INT、不改评分。

```bash
.venv/bin/python scripts/amazon_books_ratings.py sample \
  --audit-directory outputs/datasets/amazon-books-5core/full-audit-37fbebc-v1 \
  --output outputs/datasets/amazon-books-5core/engineering-s20261028-v1
```

成功子集目录包含：ratings.csv、唯一 parent_asin.csv、selected_users.csv、subset.sqlite、sample_report.json、dataset_spec.draft.json。报告记录实际 U/I/R、评分分布、双侧度统计、5-Core 状态、源版本 / 摘要、规则 / 种子 / 边界、异常与重复情况。它始终标记 engineering_validation / ratings_only_metadata_pending / model_available=false；prefix/synthetic 来源继续明确记录，不升级为完整来源。

工程用户抽样不保证商品仍满足 5-Core，必须根据真实两侧度重新判定。本流程没有迭代过滤，报告 iterative_filtering_applied=false；如需剥离低频节点，应另行设计显式步骤和前后数量记录。

草案使用新的 amazon-books-5core 数据身份、eng data/mapping 版本、preserve 1～5、topics=none、model_version=not_trained。它可由 DatasetSpec 校验，但缺少 books / 元数据及时间旁表，不能当成完整准备发布；不存在 HDFS 发布目录或可加载模型。前置 pending 草案也保存在本次本地预检查目录，真实版本待全量抽样后按摘要确定。

## 开发阶段验证（完整下载前）

- 新流程 16 项、原 Amazon Schema 13 项、DatasetSpec / 继承门禁 22 项通过，共 **51 项**。
- 人工文件覆盖：字段 / 评分、12/13 位毫秒、异常时间、重复键、整用户历史、乱序不变、最接近目标的超量前缀、等距选择、低于下限 / 单一用户超过上限、输出存在、错误源摘要 / SQLite 摘要不发布、未批准无传输、断点重试、错误 Range 和空 part。
- 实际已有片段：只读取阶段 2A 的 65,536 字节前缀，另存 65,517 字节完整行，记录未处理尾部 19 字节。未改原片段。来源仍标记 2A 的修订 / prefix，不能宣称完整核对了新固定修订。
- 片段审计：**1,129 条、38 用户、1,121 商品**，异常 0，重复额外记录 0。用户 min/median/max=3/9/309，商品=1/1/2；它不是 5-Core，也不是新工程子集。
- SQLite 索引 **102,400 字节**，这次进程峰值 RSS 24,704 KiB、审计约 0.036 秒。整个本地片段验证目录逻辑字节约 **175,027**，不从此推算完整数据资源消耗。
- 默认 10 万～30 万界限返回 infeasible，没有 ratings 子集、ASIN 工程清单或完成 manifest。完整评分及其 part 文件仍不存在。

本机检查材料位于 `outputs/datasets/amazon-books-5core/2b1-prefix-check-20261028/`，已被 .gitignore 忽略，包含预检查、pending 配置草案、片段审计及不可行报告。原始文件与第三方数据不加入 Git。

## 获批后的完整实测

用户随后明确批准仅此评分文件下载，并继续审计 / 抽样。下载实际执行成功：550,245,256 字节，SHA-256 为 `6e65a282b42894099a105f3a30dae09354e6cf423b8b81f0014b21a1fd8d3add`，修订为 `37fbebc695620f07a2dc976de770540b74a60ac6`。原 CSV 仅一份、一个硬链接；part / 下载锁已清理，没有 HF 缓存副本。源文件及成功凭据在 `data/raw/amazon-books-5core/`。

| 指标 | 完整评分 | 工程子集 |
| --- | ---: | ---: |
| 评分数 | 9,488,297 | 199,972 |
| 用户数 | 776,370 | 16,666 |
| 商品数 | 495,063 | 124,396 |
| 用户交互 min / median / max | 5 / 7 / 3,134 | 5 / 7 / 960 |
| 商品交互 min / median / max | 5 / 9 / 12,526 | 1 / 1 / 263 |
| 用户 / 商品 p95 | 32 / 56 | 32 / 4 |
| 少于 5 次交互的用户 / 商品 | 0 / 0 | 0 / 120,092 |
| 双侧 5-Core | 是 | **否** |
| 异常 / 重复额外记录 | 0 / 0 | 0 / 0 |

完整扫描结束，非法字段 / 空 ID / 越界评分 / 缺失或异常时间戳 / 重复键均为 0。12 位毫秒时间戳 104,807 条，13 位 9,383,490 条，合计与评分数一致。完整数据 UTC 时间范围为 `1996-08-17T16:01:38Z` 至 `2023-09-12T00:22:22.061Z`；工程子集为 `1997-08-14T21:30:18Z` 至 `2023-08-07T17:53:41.112Z`。

| 评分 | 完整评分条数 | 工程子集条数 |
| --- | ---: | ---: |
| 1 | 274,462 | 5,949 |
| 2 | 356,264 | 7,436 |
| 3 | 818,836 | 16,765 |
| 4 | 1,920,017 | 39,078 |
| 5 | 6,118,718 | 130,744 |

抽样按既定种子 20261028 / 用户哈希前缀执行，199,972 条是最近合法前缀，未将用户历史截成恰好 200,000 条。没有剥离低频商品，`iterative_filtering_applied=false`。独立交叉核对了全部子集行、16,666 个用户与完整审计中的逐用户历史数量、124,396 个唯一 ASIN 和选中用户清单，全部一致。该子集仅为工程验证，不能称作完整 5-Core 评价。

实际输出：

- 全量审计：`outputs/datasets/amazon-books-5core/full-audit-37fbebc-v1/`，含 audit_report.json / audit.sqlite / 空 issues.jsonl。
- 工程子集：`outputs/datasets/amazon-books-5core/engineering-s20261028-v1/`，含四字段 ratings.csv、parent_asin.csv、selected_users.csv、subset.sqlite、sample_report.json、dataset_spec.draft.json、verification.json。
- 运行总表：`outputs/datasets/amazon-books-5core/execution-summary-20261028.json`。
- 子集 CSV 大小 11,798,266 字节；SHA-256 为 `2b420672869cea96a5f7839bee762c5786b044e426428d48729a19543d782780`。
- 配置草案：data_version=`eng_s20261028_6e65a282b428_n199972`，mapping_version=`eng_m1_2b420672869c`，model_version=`not_trained`。数据状态仍为 ratings_only_metadata_pending；没有 books.csv、prepared manifest、HDFS 发布或可用模型。

资源实测：全量审计 wall time 12 分 51.28 秒、进程峰值 RSS 95,764 KiB，SQLite 为 543,272,960 字节（约 518 MiB）；抽样 wall time 21.56 秒、峰值 RSS 93,580 KiB。源文件及凭据实际占用 550,256,640 字节；完整审计目录 543,338,496 字节；工程子集目录 31,526,912 字节；开发片段目录 192,512 字节，均为文件分配字节。合计保留占用约 1.05 GiB（不等同临时峰值或累计 I/O），检查时文件系统仍可用 21,118,570,496 字节，约 19.67 GiB。time 原始资源日志在 `/tmp/bookrec-amazon-full.krLeFz/`，暂存排序 / journal 的瞬时峰值没有完整测量。

未修改源 CSV；没有下载元数据，没有自动低频过滤、训练、HDFS 写入或业务 / 集群配置修改。原 51 项人工 / 配置回归结果保留，实际完整扫描及独立子集交叉核验作为额外验证证据，不混入单元测试计数。

## 后续元数据关联

现已得到真实工程子集及含 124,396 个唯一商品的 parent_asin.csv。下一阶段单独确认元数据传输预算，流式扫描官方 Books JSONL，与清单按字符串 parent_asin 匹配；统计缺元数据、缺书名、缺作者及重复冲突，不使用子 asin 或 Kindle_Store 替代。作者取经过类型核验的 author.name；缺作者可为 null，缺书名 / 元数据默认阻止完整发布，不伪造商品描述，不删评分来通过校验。时间旁表与已有稳定 ID 映射需在之后的专属准备入口中同批绑定。
