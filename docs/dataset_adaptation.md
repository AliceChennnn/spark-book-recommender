# 数据集配置与标准化适配接口

**系统默认使用 Goodbooks-10k，另外提供可扩展的数据集配置与标准化适配接口，支持未来接入其他具有用户—图书评分关系的数据集。新数据集训练、正式评价和 WebUI 动态切换属于后续扩展，不包含在当前已实现功能中。**

Goodbooks-10k 仍是默认且已完成的数据集，沿用原有数据、模型、CLI、画像、ALS / Popularity 推荐、WebUI、13 份演示及评价结果。当前默认运行链路没有改为通过 DatasetSpec 加载，不能把新配置接口解释成已支持任意数据集的一键运行。

本分支的收尾目标是清晰、安全、可复用的适配边界，不是完整接入 Amazon。已有 Amazon 评分、工程子集、审计结果和人工测试作为探索材料保留；停止后续 Amazon K-Core 抽样、元数据下载、模型训练及评价开发，不将这些工作列为本分支完成条件。[2A](amazon_books_2a.md) 和 [2B-1](amazon_books_2b1.md) 中的历史后续步骤不代表当前开发计划。

首次实现记录：从 `feature/user-preferences` 的 `b6ac3007c2e9064da4b7c6a53ccfcb2e57dc5302` 创建 `feature/dataset-switching`。本次定位收尾以当前 HEAD `09d18ea7980a6079e067c4f9999a79222423f728` 为检查起点，保留已有未提交探索文件，不提交、不修改 main。本轮仅检查接口、运行人工回归和更新文档，不下载数据、训练模型或操作正式 HDFS 产物。

## 当前接口清单

| 接口 | 已实现的职责 | 使用边界 |
| --- | --- | --- |
| RatingScale | 明确源 / 目标范围及 preserve / linear；拒绝非有限、反向或隐式量表 | 只适用于数值型显式评分，不替操作者决定隐式反馈或缺失值含义 |
| DatasetSpec.from_dict / preparation_config | 声明来源字段、数据 / 映射 / 模型版本、topics；字段映射只读，导出配置为独立 JSON 字典 | 不选择在线数据源；Goodbooks 前缀保留给原有入口，防止重新编号或改写原产物 |
| IdMappings / adapt_frames | DataFrame 校验、字段投影、来源字符串到正 INT 的可追溯映射；显式 create / extend | 纯内存接口不检查发布目录，持久化须经 prepare 的谱系门禁 |
| AdaptedDataset.ratings / books / mappings | 返回统一评分、图书表和来源 ID 映射表 | 额外字段不自动保留；时间戳等需由专属适配器另行设计 |
| AdaptedDataset.summary / preview_popularity | 基础计数 / 评分范围；按全部观测评分计数的工程热门预览 | 不是训练集 Popularity 基线、ALS 或正式 Top-K 评价 |
| prepare_dataset.load_spec / prepare / CLI | 读取两份显式 CSV，检查新目录及映射继承，输出 Parquet / 映射 / 准备清单 | 离线数据准备入口，不训练、不切换页面、不运行正式评价；本轮不对真实数据执行 |
| data_binding / model_binding / cache_version / require_artifact_binding | 生成隔离身份并校验调用方提供的产物绑定 | 模型路径和版本标识不是已存在的模型；尚无通用模型训练 / 加载工厂或新数据集缓存接线 |

## 标准 Schema 与配置覆盖

| 表 | 内部字段 | 已校验的内容 |
| --- | --- | --- |
| ratings | user_id INT、book_id INT、rating DOUBLE | 来源 ID 非空、映射无冲突、评分有限且在声明范围内、用户—图书键不重复、图书引用可关联 |
| books | book_id INT、title STRING、authors STRING（可为空） | 图书 ID 唯一、书名非空；作者可配置为来源列或 null |
| mappings | source_id STRING 与 user_id / book_id INT | 来源原文可反查、顺序稳定、继承时旧 ID 不变、新 ID 追加、INT 上限检查 |

上述格式足以承载基础评分分析及显式评分 ALS 的 userCol / itemCol / ratingCol 输入。它不意味着已经给新数据训练了模型，或已有可复现的训练 / 测试划分与评价协议。

不同列名、评分端点与变换、数据集 / 数据 / 映射 / 模型版本均由配置声明；create / extend 由入口参数明确声明，后者必须指定最新前序配置。因而更换兼容来源时可先完成专属格式解析，再向标准接口提供 DataFrame，不必把原始字段名带入核心 ALS 运算。嵌套 JSONL、自定义重复政策、特殊 ID / 时间语义仍需专属适配器，不能只改字段映射后猜测处理。

当前新来源接口明确支持 topics=none。摘要返回 topics_available=false 和“主题画像暂不可用”，热门预览也返回该标记；不读取、生成或冒用 Goodreads 标签。当前 Goodbooks 主题画像链路仍使用原标签数据，没有为其他量表或无标签数据集完成通用画像接入。

## 已实现与后续扩展

| 能力 | 当前状态 |
| --- | --- |
| Goodbooks 数据、模型、分析、画像、推荐、页面和既有评价 | 已完成，继续为默认行为；本分支收尾不改动 |
| 配置声明、标准化显式评分、可空作者、无标签标记、稳定 ID 与版本隔离 | 已实现并有人工测试 |
| 独立 CSV 数据准备、默认拒绝覆盖、显式映射继承与发布绑定检查 | 已实现；没有据此声称真实新数据集已完整接入 |
| 模型 / 缓存的版本身份 | 已实现标识及绑定校验接口；实际训练和运行时选源尚未对接 |
| 运行时数据集注册 / 选择、通用 ALS 训练 / 推理入口、新数据集正式评价 | 后续扩展，当前未实现 |
| WebUI 动态切换、图表 / 画像量表通用化、专属可选主题适配 | 后续扩展，当前未实现 |
| Amazon 完整接入 | 不属于当前分支目标；保留已有探索材料，停止后续开发 |

剩余限制：CSV 入口只读带 header 的固定 ratings.csv / books.csv；来源列名需先规范为简单字段名，来源 ID 必须为 STRING；只提供 preserve / linear，不处理隐式反馈。旧画像仍假定整数 1～5，旧训练 / 评价脚本仍绑定 Goodbooks 路径和实验规则。版本由操作者声明，不自动检测外部数据变动；发布无跨目录事务或并发锁，同一 mapping_version 必须串行准备。以上边界不影响当前 Goodbooks 已完成流程。

以下保留逐文件分析、目录规划和历史验证细节；其中未来接入条件仅为接口边界说明，不是必须继续实施的开发清单。

## 耦合点与最小修改边界

| 文件 / 模块 | 当前耦合 | 复用及适配边界 |
| --- | --- | --- |
| inspect_data.py | 固定 raw 路径、INT ID、整数 1～5 分、Goodbooks CSV 列名 | 计数、缺失值、重复键检查可复用；原入口仅用于 Goodbooks，新适配器先校验来源字段 / 量表 |
| prepare_data.py | 固定 processed 路径；转换 average_rating、ratings_count、publication year，并保留 Goodreads 字段 | 保留旧逻辑；新入口只承诺统一评分与最小图书元数据，不猜测其他数据的额外字段 |
| prepare_tags.py | Goodreads edition ID 到内部 book_id、tag_id / count 字典、topic 白名单 | 是专属适配器；其他来源不能假装具备同样标签结构。现有 max 去重和异常门禁不变 |
| train_als.py、tune_als.py | 固定输入；user_id % 20 试验用户、80/10/10 划分、固定参数候选 | ALS 运算可复用，抽样、调参及划分需由未来实验协议明确。tune 会 fit，当前不运行 |
| train_final.py | 固定正式模型路径；排除 pilot 用户的 INT xxhash64 测试划分、固定超参数 | 不把旧划分 / 参数直接用于新数据；保留原 --overwrite 保护，不运行训练 |
| evaluate_topk.py | 固定正式模型、100 个合格用户、评分 ≥4、K=10、同一哈希划分 | 新数据需明确量表、正样本、用户资格、候选和排除口径；本轮不新建正式实验 |
| recommenders.py | 推荐排序 / 已评分排除通用；from_hdfs 固定 legacy 路径，Popularity 绑定旧训练划分 | 核心推理可复用；未来工厂必须显式接收同数据 / 映射 / 模型绑定及对应训练集，不能直接调用旧工厂换根目录冒充适配 |
| preferences.py | 小型历史汇总可复用；整数 1～5、≥4 为高分、rating-3 贡献、五档分布 | 任意实数量表不能直接交给旧画像函数；阈值 / 分布 / 贡献规则待选定数据后明确。现有无主题信息提示可参考 |
| analyze_preferences.py | 默认读 legacy ratings / books / Goodreads topics | 虽有 root/topics 参数，仍假定主题表存在、量表兼容，不是通用数据集入口 |
| analyze.py、visualize.py | 统计 SQL 大多通用；固定路径、最低 100 条评分、活跃度分组、导出 / 图片路径 | 新框架提供基本统计，复杂业务阈值后续配置；不运行旧导出以免覆盖 |
| web_backend.py | BOOKREC_DATA_VERSION 纳入代码指纹；TTL 3600；v1 快照校验；概览及输出目录固定 | 版本标记只隔离缓存，不切换读取源。保留现状；未来集成必须同时选择来源和独立缓存命名空间 |
| web_job.py | 容器内固定 legacy 数据 / 模型 / topics 路径，调用旧算法 | 页面切换前必须新增受校验的数据集参数 / 注册表，不能仅改环境版本 |
| app.py | 当前用户示例、五档画像、旧演示布局，未选择数据集 | 本轮不重写页面；继续提供原 13 份演示 |
| 测试 / scripts / infra | 验收与冻结模型审计绑定旧路径、规模、ID 和数值；启动依赖既有挂载 / 网络 | 历史测试与指标继续属于 Goodbooks；新测试只用人工数据。部署及集群脚本不修改 |

意外覆盖风险：旧 prepare_data / train_als / train_final 已有默认拒绝、显式 --overwrite；该授权仍有破坏性，本轮不使用。prepare_tags / 画像输出要求新目录。analyze.py 仍无条件覆盖 analysis，visualize.py 覆盖图表；演示注册会更新运行索引、运行缓存会原子替换缓存文件。本轮不运行以上业务入口，也不修改它们的策略。

## 新增配置和目录

`src/datasets.py` 负责字段声明、显式评分变换、产物绑定及路径生成；`src/dataset_adapter.py` 负责人工 / CSV 来源的 DataFrame 适配；`src/prepare_dataset.py` 是独立准备入口。没有建立虚构的真实数据集注册项。

Goodbooks 原根目录仍为 `hdfs://master:9000/user/root/bookrec`，旧 processed / models 不迁移。新配置禁止 goodbooks 前缀、目录穿越、任意 output-root 及未知配置键；只生成以下子目录，不允许覆盖发布目录：

```text
bookrec/datasets/<dataset_id>/
  raw/<data_version>/ratings.csv, books.csv       # 操作者另外准备；本脚本只读
  releases/<data_version>/<mapping_version>/
    processed/ratings                           # INT, INT, DOUBLE
    processed/books                             # INT, STRING, nullable STRING
    mappings/users                              # source_id STRING, user_id INT
    mappings/books                              # source_id STRING, book_id INT
    manifest                                    # 最后写入的单行 JSON / 发布完成标记
    models/<model_version>/                     # 仅规划；当前不训练、不创建模型
```

本机新产物建议位于 `outputs/datasets/<dataset_id>/<data_version>/<mapping_version>`，缓存再按 `spec.cache_version` 分隔。此属性只是命名空间规划，当前未在 WebUI 创建该目录，也不把新版本接入旧快照索引。

配置示例**仅描述人工测试夹具**，不是已选择或已下载的第二个数据集：

```json
{
  "dataset_id": "fixture-a",
  "data_version": "r1",
  "mapping_version": "m1",
  "model_version": "als1",
  "ratings_columns": {"user_id": "reader", "book_id": "isbn", "rating": "score"},
  "books_columns": {"book_id": "isbn", "title": "name", "authors": null},
  "rating_scale": {"source_min": 0, "source_max": 10, "target_min": 1, "target_max": 5, "transform": "linear"},
  "topics": "none"
}
```

CSV 第一阶段只支持带 header 的逗号分隔输入。配置列名明确、不能一列多用；缺失列、空 ID、重复评分键、重复图书 ID、缺失图书引用或标题、不可解析 / 非有限 / 越界评分直接失败，不选择平均、最新、截断或丢弃等未经约定的策略。authors 可映射到来源列，也可明确为 null。

评分只接受明确的 preserve 或 linear：preserve 要求两端量表相同，linear 按声明的端点线性变换，保留实数，不偷偷取整。0～10 转 1～5 必须明确配置；0 是合法分、隐式反馈还是缺失占位符，要等真实数据说明确定，不能默认解释。

## ID、模型和缓存隔离

来源 ID 必须为非空 STRING，保持原文；`001` 和 `1` 不合并，不把 ISBN / UUID / 任意字符串 cast 为 INT。首次对唯一原始 ID 按确定性字典序分配正 INT，内部 user / book 分别建表，可由映射反查，检查重复、冲突和 INT 上限。

映射在一个不可变发布批次内稳定，与输入行顺序无关。导入入口必须显式指定 `--mapping-mode create` 或 `--mapping-mode extend`，库函数 prepare / adapt_frames 同样要求传入 mapping_mode，不再把缺少前序配置自动当作首次创建。

- create：首次创建一个尚未使用的 mapping_version，不能提供 previous-config。写入前通过 Hadoop 文件系统元数据检查 `releases/*/<mapping_version>`；任何旧数据批次或部分目录已使用该版本时均拒绝，即使本次 data_version 不同。
- extend：必须提供 `--previous-config`，且数据集 / 映射版本相同；读取并验证既有发布清单，要求前序配置指向唯一最新映射批次。保留所有旧 ID（包括新输入暂未出现的来源 ID），新 ID 仅在旧最大值后追加。不按 data_version 字典序猜“最新”，避免 r10 / r2 顺序错误。
- 发布清单记录 mapping_mode 和 mapping_parent 绑定，目录结构不变。单个旧格式发布清单可作为首次创建的根；多个无父记录的旧批次、分叉、断裂 / 循环谱系、缺失或不可读的清单一律拒绝，不能静默重新编号。过旧的 previous-config 也拒绝，防止遗漏后续已分配的 ID。

若重建编号，应使用新的 mapping_version，旧模型随之不可复用。纯内存 adapt_frames 不查询发布目录，持久化导入须经 prepare 的历史门禁。全局排序仍使用原 Spark 窗口，本轮不优化 ID 映射性能。

DatasetSpec 的 ratings_columns / books_columns 使用复制后封装的只读映射，实例创建后不能直接增删或替换字段，调用方原字典修改也不影响实例。preparation_config 返回独立的普通 JSON 字典，可安全导出；修改导出副本不会改变配置、产物绑定或缓存标识。评分配置仍为冻结对象；既有配置的摘要和路径不变。

prepared manifest 绑定数据集、数据批次、映射版本及适配配置摘要（包含 adapter_version）；模型绑定在此基础上再包含 model_version。模型版本变化不会搬迁准备数据；来源字段或量表变化会改变绑定。`require_artifact_binding` 是后续读数据 / 模型前必须调用的门禁，本阶段只实现校验接口，尚未提供通用 ALS 加载 / 训练入口。

cache_version 同时包含数据与模型绑定，不同数据集 / 批次 / 映射 / 量表 / 模型不会得到相同标记。它不代替现有代码指纹、TTL 或快照摘要，也不自动检查 HDFS。**不能仅把此值设置为 BOOKREC_DATA_VERSION 就认为 WebUI 已切换来源**；页面和桥接还未接入新配置。旧模型、训练集与旧推荐缓存均不供新入口使用。

没有标签的输入可完成准备、基础统计和 `AdaptedDataset.preview_popularity`。后者按全部观测评分次数排序并排除已评分图书，结果明确标为 `observed_popularity_preview`，不是正式 train-only Popularity 或新的 Top-K 实验，不读取 ALS。摘要及预览标明主题画像暂不可用；不向旧五档画像函数传入未知量表。

## 使用边界与验证

本机可只读查看帮助，不需要 PySpark：

```bash
.venv/bin/python src/prepare_dataset.py --help
```

未来由操作者备齐真实来源和配置后，才可使用以下入口；**本轮没有执行实际数据准备**：

```bash
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  src/prepare_dataset.py --config /workspace/bookrec/<已审阅配置.json> --mapping-mode create
# 同一映射体系扩充数据：改用 --mapping-mode extend，并提供
# --previous-config /workspace/bookrec/<最新映射批次配置.json>
```

新入口故意不提供 --overwrite 或自由 output-root；原入口的 --overwrite 不变。目标批次目录存在（包括部分失败目录）立即拒绝，读取原始数据之前检查。所有写入使用 errorifexists，manifest 最后写入；失败不删除或覆盖任何目录。没有跨路径事务或并发发布锁；**同一数据集 / mapping_version 必须串行导入，即使 data_version 不同**，避免历史检查与提交之间的竞争。部分失败目录或旧的分叉历史需要另行人工审计，不能直接跳过或删除；可能需要新的 mapping_version。发布清单与既有映射文件应由操作者保持完整，不把外部手动改写当作合法继承。

本轮测试命令：

```bash
.venv/bin/python -m unittest discover -s tests -p test_datasets.py -v
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master 'local[2]' tests/test_dataset_adapter.py
.venv/bin/python -m unittest discover -s tests -p 'test_web*.py' -v
```

Spark 测试只使用人工 DataFrame / file:///tmp 新目录。验证统一 Schema、字段别名、0～10 显式变换、缺标签及作者、重复键、空 ID / 元数据、非法评分、字符串映射稳定与增量继承、映射冲突、发布清单、拒绝覆盖及原始 CSV 不变。配置测试验证路径、量表、数据 / 模型绑定与缓存命名空间隔离。

2026-10-28 结果：配置 / 导入 13 项、人工 Spark 适配 8 项、原 WebUI / 缓存 22 项通过，共 43 项；git diff --check 通过。HDFS processed / models 递归清单前后一致，61 份历史审计 / 演示文件摘要未变，演示索引逐字节未变。13 份演示请求均命中预计算结果；在线 WebUI 保持原进程，loopback 和 NAT 地址健康检查均为 HTTP 200。本次未重新验证 Windows 浏览器，也未将人工测试当作真实第二数据集验收。

随后定向修复的回归结果：配置 / 不可变性 / 继承门禁 22 项、人工 Spark 适配 9 项通过，共 31 项。新增回归在 file:///tmp 构造旧发布，验证同 mapping_version / 不同 data_version 时遗漏 previous-config、重新 create、选用过旧前序均在读取新 CSV / 写入之前拒绝；合法 extend 保留旧用户 / 图书 ID 和暂未出现的映射，新 ID 在旧最大值后追加，旧目录摘要不变。既有配置绑定及缓存标识与修复前固定值一致。此轮不重跑真实导入或训练，也不做 ID 映射性能优化。

本次定位收尾重新运行上述三组测试：配置与版本 / 继承门禁 22 项、人工 Spark 适配 9 项、原 WebUI / 缓存 22 项，共 53 项全部通过。已有覆盖足以验证当前接口，不新增业务代码或重复测试。测试日志有 PySpark socket ResourceWarning 和 Streamlit / Pandas 弃用提示，无测试失败；没有运行模型训练或真实来源准备。

本次只读完整性核对：21 份 Amazon 数据 / 输出文件的清单、大小及修改时间不变；60 份受保护的历史审计 / 演示文件 SHA-256 不变，包含原演示清单及 13 份快照。13 份请求均命中 precomputed，禁止计算的后端未被调用；既有 WebUI loopback 健康检查为 HTTP 200。未扫描正式 HDFS，本次验证不等于重新验收集群产物或 Windows 浏览器访问。

## 可选的未来扩展点（不属于本分支完成条件）

- 真实格式、编码 / 分隔符、字段类型、缺失值、评分范围和显式 / 隐式语义。
- 重复评分的时间戳与取舍政策、书目 ID 合并政策，以及稳定 ID 映射发布 / 增量策略。
- 数据许可、实际来源及资源规模；真实元数据、作者和可选标签的适配。
- 新训练 / 验证 / 测试划分、正样本口径、ALS 参数、模型来源绑定及独立评价计划。
- 实数量表的用户画像阈值 / 分布，受控运行时数据集注册、推荐工厂、WebUI 来源与缓存同步切换。

已有 Amazon 探索不改变系统默认数据集，也不要求现在继续 K-Core、元数据、训练、评价或页面切换。

本阶段不改变正式指标、不训练 Goodbooks ALS、不写生产 HDFS、不改集群或网络配置，也不修改任何原始演示 JSON 或运行时演示清单。
