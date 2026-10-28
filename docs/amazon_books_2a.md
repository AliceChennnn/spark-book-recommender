# 阶段 2A：Amazon Reviews 2023 Books 5-Core 来源与接入准备

当前定位：本文件是已保留的历史探索记录。系统仍默认使用 Goodbooks-10k，Amazon 完整接入不作为 feature/dataset-switching 的完成目标；停止后续抽样、元数据下载、训练和评价开发。下文历史“下一阶段”不再作为当前计划，范围以 [数据集适配接口说明](dataset_adaptation.md) 为准。已有文件与审计结果不删除。

检查日期：2026-10-28（Asia/Shanghai）。分支 `feature/dataset-switching`，起点 HEAD `09d18ea7980a6079e067c4f9999a79222423f728`；开始时工作区干净。未修改 main / feature/user-preferences，未 commit / push / merge。

本轮只做来源、容量与格式检查、设计及人工样例测试。没有完整下载、真实导入、HDFS 写入、训练、正式评价或 WebUI 改动。只使用 Books 来源，不使用 Kindle_Store。原始文件和真实前缀片段不进入 Git。

## 官方来源与实际访问

| 来源 | 本轮结果 |
| --- | --- |
| [官方主页](https://amazon-reviews-2023.github.io/) | 网页可读，含字段及类别说明 |
| [官方 5-Core 页面](https://amazon-reviews-2023.github.io/data_processing/5core.html) | 网页可读；Books 约 776.4K 用户 / 495.1K 商品 / 9.5M 评分，是官方舍入统计，不是本机精确计数 |
| [指定评分 gzip](https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/benchmark/5core/rating_only/Books.csv.gz) | 本机 urllib HEAD 出现 SSL UNEXPECTED_EOF；curl HTTP/1.1 HEAD 为 SSL_ERROR_SYSCALL，未收到目标文件 HTTP 响应 |
| [指定元数据 gzip](https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Books.jsonl.gz) | 同样 TLS 连接失败，未取得 Content-Length；没有关闭证书验证 |
| [官方实验室 Hugging Face 仓库](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023) | 从官方主页链接确认发布者；目录 / 版本 API 可访问，两个 Books 文件 Range GET 均为 HTTP 206 |
| 官方 HF 卡片链接到的 datarepo.eng.ucsd.edu 元数据 gzip | HEAD 为 404；同站等价 Books 评分 gzip 路径探测也为 404，不作为已验证备用下载地址 |

curl 中的 `200 Connection established` 是代理 CONNECT 响应，不能记为数据文件下载成功。当前已验证的数据正文来源是实验室自己的 HF 镜像。没有使用第三方再发布数据集，也没有执行 HF loading script 或安装额外依赖。

官方镜像修订为 `2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e`。未来下载应固定该修订及文件身份，检查最新响应；不依赖可变 main。原 UCSD gzip 与 HF 明文文件尚未做全量内容等价核对，不能声称压缩字节或文件头完全相同。

## 容量与磁盘

| 文件 | 官方镜像声明字节 | GiB | 远端声明 SHA-256（尚未全量校验） |
| --- | ---: | ---: | --- |
| [5core/rating_only/Books.csv](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/blob/2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e/benchmark/5core/rating_only/Books.csv) | 550,245,256 | 0.512456 | 6e65a282b42894099a105f3a30dae09354e6cf423b8b81f0014b21a1fd8d3add |
| [meta_Books.jsonl](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/blob/2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e/raw/meta_categories/meta_Books.jsonl) | 14,697,581,022 | 13.688189 | 3b008960580e390874b6723d438a3df3558bc98b9547ed37c90dc1659109cec7 |
| 合计明文 | 15,247,826,278 | 14.200645 | 不是 gzip 下载量 |

项目路径和 /tmp 共用一个文件系统；检查时可用 22,244,503,552 字节，约 **20.717 GiB**，不能重复计算两者空间。两个明文文件若都落地，只剩约 6.516 GiB，尚未计入 staging、Spark 临时文件、Parquet、HDFS 副本和失败重试；不足以据此保证完整流水线容量。原 gzip 压缩大小未知，不从其他类别或经验压缩率推算。

本轮每个文件只读取首部 65,536 字节，共 **128 KiB 原始数据正文**；Range 被忽略也会硬性停止读取。片段及响应记录只保存在本机 `/tmp/bookrec-amazon-2a.LQ8trb/`，是临时检查材料，不能视为完整文件或持久备份。

未来容量确认建议：先单独批准约 550 MB 的评分明文镜像下载到 `data/raw/amazon_books_2023_5core/<revision>/Books.csv`，用于全量质量检查和确定性抽样。元数据另行批准：已知明文传输约 14.7 GB，优先核实 gzip 的可访问性和大小，再采用流式解压 / 投影，避免整份明文与复制件同时落地。**流式扫描整个远端元数据也属于大文件全量传输，仍需先确认**；本轮没有执行。

## 实际 Schema 与证据边界

### 评分

官方 5-Core 页面及官方 HF Books CSV 首部一致：

| 来源字段 | 实际 / 目标类型 | 处理 |
| --- | --- | --- |
| user_id | STRING | 非空，保留原文；不转为数值来源 ID |
| parent_asin | STRING | 非空的父商品 ID；作为 book 来源键，不改用子 asin、ISBN 或行号 |
| rating | DOUBLE | 显式 1～5；preserve，不缩放或取整 |
| timestamp | 原文整数 → LONG Unix 毫秒 | 全程保留，禁止按位数猜单位或静默乘除 1000 |

64 KiB 片段含 1,129 条完整评分，涉及 38 个用户、1,121 个商品。四列齐全；片段中空 ID、非法评分、无效 / 缺失时间戳、重复 user/item 键均为 0。评分最小 1.0、最大 5.0；时间戳有 **12 和 13 位**，按毫秒解释覆盖 2000-12-10 至 2023-03-06。不能强制要求所有毫秒值都是 13 位。

Range 在下一行中途结束的 19 字节单独记录为未处理尾部，不计为来源异常或完整评分。片段连续且靠近文件开头，**不是实验子集、随机样本或全量审计**。

[官方处理说明](https://github.com/hyp1231/AmazonReviews2023/blob/main/benchmark_scripts/README.md) 声明先按 user/item 去重并保留最早交互，再执行 k-core 过滤。不能因此跳过完整文件的重复检查。官方处理脚本的写文件代码与已发布 HF 文件的 header 存在版本差异，实际下载仍须检查 header，不能自动把首条数据当表头丢弃。

### 图书元数据

实际读取格式为 UTF-8 JSONL：每行一个商品对象；与 5-Core 评分关联使用 **parent_asin**。元数据覆盖原 Books 类别，并非只包含 5-Core / 工程子集中的商品。

64 KiB 片段含 29 个完整对象：parent_asin / title 均为非空字符串；没有片段内重复商品。author 为对象 21 条、null 8 条；对象有 `name / avatar / about`，name 在这些对象中为字符串。投影使用 `author.name → authors`，缺失作者保留 null，不能把 store、Publisher 或 JSON 对象字符串化当作者。片段尾部 2,155 字节未形成完整 JSON，单独记录，没有视为损坏来源。

片段还出现 subtitle、main_category、average_rating、rating_number、features、description、images、videos、categories、details、price、store、bought_together 等字段。它们不是本阶段 ALS 输入，也不将 Amazon categories 伪造成 Goodreads 主题特征。

### 尚不能声明的全量质量结果

| 检查项 | 当前能确认 | 下一阶段必须补齐 |
| --- | --- | --- |
| 四字段、ID、评分、时间戳 | 官方格式及首部片段 | 全文件字段宽度、空值、类型、范围、时间上下界与异常计数 |
| user/item 重复 | 官方声明去重；片段无重复 | 精确重复键组数、额外记录数、同键评分 / 时间冲突 |
| 用户 / 商品 / 评分规模 | 官方舍入规模 | 全文件精确 distinct / 行数及双侧度分布 |
| 元数据缺名、缺作者、重复 | 29 条片段的上述计数 | 全 Books 元数据及选中商品范围内分别审计 |
| 无法关联评分商品 | 官方主页提示部分商品缺元数据 | 对所选评分商品做全元数据扫描后的 left-anti join；两段独立首部不能证明关联覆盖率 |
| 5-Core 条件 | 官方数据产品声明 | 清点唯一评分键后验证每个用户 / 商品至少 5 个交互 |

未猜测异常比例；未把片段中的 0 外推为全量 0。

## 接入方案与通用框架边界

现有 DatasetSpec 已可声明 Amazon 字段、preserve 1～5 量表和独立版本；映射与 create/extend 门禁继续复用。**prepare_dataset.py 尚不能直接接收这两个官方文件**：它固定读 ratings.csv / books.csv，不读取 JSONL author 对象，adapt_frames 只投影三列评分并会遗漏 timestamp。

建议下一阶段增加独立 Amazon 准备入口，通用算法不重构：

1. 对原四字段先全量审计，来源固定为 Books 5-Core；gzip 评分用正确压缩后缀 / 解压流读取，明确 header。保留原文件及全部四字段的可追溯 staging。
2. 显式解析 Books JSONL，投影 parent_asin / title / author.name；按选中商品 ID 筛选所需元数据，不通过内部 book_id 连接来源表。先审计重复商品和类型冲突，不随意 distinct 选一条。
3. 复用当前发布目录检查、映射版本谱系门禁和 adapt_frames；用户 / 商品来源 ID 均以字符串参与稳定映射。不同 data_version 扩充同一 mapping_version 时必须 extend 最新发布。
4. 输出 canonical ratings(user_id INT, book_id INT, rating DOUBLE) 和 books(book_id INT, title STRING, authors STRING nullable)。另写时间旁表，例如 interactions_time(user_id INT, book_id INT, timestamp_ms LONG)，与 ratings 键逐一对应、行数一致；保留来源四字段及源文件 / 行定位信息供核对。旁表应与同一发布 manifest 绑定并在发布前验证，不能在通用投影之后才想起恢复时间。
5. 日期单位固定为毫秒。原型按官方声明的 1996-05 至 2023-09 时间窗口检查，窗口外记录先报告复核，不猜单位、填时间或静默丢弃；最终导入策略在完整审计后确定。

**缺元数据 / 书名策略：默认审计后阻止发布，保留全部评分和原始记录。** 不通过 inner join 删除评分来满足校验。若确需保留可推荐目录，可另行批准可见的“元数据缺失”占位项，并保存 metadata_status / 来源 / 异常清单；这不是本轮已实施的策略。缺作者本身可允许 null；同 parent_asin 的重复元数据若有冲突，须明确合并 / 选择政策后再发布。

无主题标签时可以基础统计及 Popularity 工程验证，明确主题画像不可用。新数据不使用 Goodbooks 标签、权重、训练集、划分规则或缓存。当前不更改 WebUI，也不能只设置 BOOKREC_DATA_VERSION 就声称页面切换到了 Amazon。

建议身份：dataset_id=`amazon_books_2023_5core`；工程样本 data_version=`eng_s20261028_v1`、mapping_version=`eng_m1`、model_version=`not_trained`。完整数据使用另外的 full data / mapping 版本。目录继续沿用 `bookrec/datasets/<dataset_id>/raw/<data_version>` 及 `releases/<data_version>/<mapping_version>`；模型、缓存和 manifest 同数据绑定隔离。上述仅为规划，当前没有创建真实配置发布或 HDFS 目录。

本轮新增 `src/amazon_books_schema.py` **行级原型**：验证四字段 / preserve 评分 / 毫秒时间；将已观察到的 author 对象投影到可空 authors，缺书名显式返回问题；提供可复现用户哈希。可接受 Python gzip 文本流，但不下载、不执行 Spark、不发布目录，也未实现全量去重、覆盖率审计或时间旁表。因此它不是可立即执行的批量导入器。

## 10 万～30 万条评分的工程子集方案

目标约 20 万条，允许 10 万～30 万条。当前没有实际抽样；实际用户数 / 商品数 / 评分数、5-Core 状态均待下载批准后的扫描，不预填或猜测。

1. 完整评分质量门禁通过后，统计每个来源用户的交互数。按 SHA-256 用户键排序，并以原始 user_id 作为碰撞时的确定性次序；种子固定 **20261028**。
2. 哈希输入严格为 UTF-8 字节：`amazon-books-2023-5core-user-v1` + NUL + 十进制 seed + NUL + 来源 user_id。对应原型 user_sample_key；不使用 Python 的进程随机 hash，不依赖 CSV 行序或内部 INT 编号。
3. 沿排序后的用户前缀计算累计评分数，选取落在 [100000, 300000] 且最接近 200000 的前缀；同距离时选较小前缀。保留这些用户的完整评分历史，不截某个用户的行。如果没有合法前缀，报告方案不可行并另定新版本规则，不悄悄跳过重用户或截断。
4. 在全评分中按选中用户提取四字段，核对累计数与输出数一致；扫描 Books 元数据提取所需商品，不额外下载 Kindle_Store。元数据缺失仍按上文门禁处理。
5. 记录规则版本、seed、源修订 / 实际文件摘要、用户阈值或所选用户清单摘要、精确 U/I/R、时间范围、重复 / 非法记录计数，以及两侧 degree 的 min / median / p95 / max / 小于 5 的节点数。
6. 原完整 5-Core 经用户抽样后，商品交互数可能降到 5 以下；不能继续标为满足 5-Core。用唯一交互键验证两侧最小度，实际记录 `is_5core`。人工 K5,5 样例已证明：选两个用户完整历史后用户度仍为 5，商品度却为 2。
7. 本轮优先保留完整用户历史并如实报告非 5-Core 工程子集。若后续必须得到 5-Core 子集，再另行批准双侧反复剥离到收敛，记录每轮损失；结果可能过小或为空，不保证既保留 5-Core 又达到指定规模。

样本身份应明确 `scope=engineering_validation`。它只能验证读取、转换、映射、时间保存和资源占用；将来产生的指标也不能冒充完整 Books 5-Core 正式评价。不在本轮决定或运行新的 ALS / Top-K 实验。

## 测试与后续步骤

13 项 Amazon 人工测试、22 项现有 DatasetSpec / 继承门禁回归均通过，共 **35 项**。人工测试覆盖 gzip 文本流、四字段时间保留、非法 ID / 评分 / 时间、空行 / header、作者对象及缺失、元数据缺口不删评分、重复记录不静默去重、稳定抽样键及抽样后不保证 5-Core。测试全部在内存 / 临时目录，不联网、不启动 Spark。

下一阶段顺序：

1. 确认下载来源（优先可验证的官方固定修订镜像）、实际文件大小、保存路径及磁盘预算，获批准后才传输全量评分。原 gzip 大小若仍取不到，不能写成已验证容量。
2. 只读全量评分审计，取得精确规模、时间 / 重复质量与 5-Core 度信息；异常非零时先报告政策，不自动修复。
3. 按固定规则产生工程样本并记录真实规模。抽样本身需要扫描全部评分，不能用本轮 64 KiB 片段替代。
4. 单独确认元数据传输预算，流式全扫 / 投影后做全量和样本所需商品的覆盖 / 重复 / 缺名 / 缺作者审计，决定缺失元数据政策。
5. 实现 Amazon 专属准备入口与时间旁表、独立新发布路径和上述门禁；先仅本地工程验证。HDFS 导入、模型训练、正式评价及 WebUI 切换分别作为后续授权工作。
