# 多数据集适配：第一阶段

基线：`feature/user-preferences` 的 `b6ac3007c2e9064da4b7c6a53ccfcb2e57dc5302`。开始时工作区干净，本地和认证远端分支一致，目标分支不存在；从此提交创建 `feature/dataset-switching`。未修改 main，未 commit / push / merge。

本阶段只有人工测试数据，不假定第二个真实数据集已可用，也不下载数据。保留 Goodbooks 原入口、产物和实验规则，新增独立的配置与 CSV 适配入口。尚未实现完整的新数据集训练、评价或页面切换。

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

映射在一个不可变发布批次内稳定，与输入行顺序无关。对同一映射体系扩充数据时，必须用 `--previous-config` 读取已发布且绑定一致的映射，保留原 ID、新 ID 在旧最大值后追加；不能在同一 mapping_version 下忘记旧映射后重新编号。若重建编号，应使用新的 mapping_version，旧模型随之不可复用。全局排序目前使用 Spark 窗口，适合建立正确性基线；大规模词典的性能优化待真实规模明确。

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
  src/prepare_dataset.py --config /workspace/bookrec/<已审阅配置.json>
# 同一映射体系扩充数据：另加 --previous-config /workspace/bookrec/<旧配置.json>
```

新入口故意不提供 --overwrite 或自由 output-root；原入口的 --overwrite 不变。目标批次目录存在（包括部分失败目录）立即拒绝，读取原始数据之前检查。所有写入使用 errorifexists，manifest 最后写入；失败不删除或覆盖任何目录。没有跨路径事务或并发发布锁，重试应使用新的批次并核对映射继承，避免同时准备相同数据集 / 批次。

本轮测试命令：

```bash
.venv/bin/python -m unittest discover -s tests -p test_datasets.py -v
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master 'local[2]' tests/test_dataset_adapter.py
.venv/bin/python -m unittest discover -s tests -p 'test_web*.py' -v
```

Spark 测试只使用人工 DataFrame / file:///tmp 新目录。验证统一 Schema、字段别名、0～10 显式变换、缺标签及作者、重复键、空 ID / 元数据、非法评分、字符串映射稳定与增量继承、映射冲突、发布清单、拒绝覆盖及原始 CSV 不变。配置测试验证路径、量表、数据 / 模型绑定与缓存命名空间隔离。

2026-10-28 结果：配置 / 导入 13 项、人工 Spark 适配 8 项、原 WebUI / 缓存 22 项通过，共 43 项；git diff --check 通过。HDFS processed / models 递归清单前后一致，61 份历史审计 / 演示文件摘要未变，演示索引逐字节未变。13 份演示请求均命中预计算结果；在线 WebUI 保持原进程，loopback 和 NAT 地址健康检查均为 HTTP 200。本次未重新验证 Windows 浏览器，也未将人工测试当作真实第二数据集验收。

## 等待选定第二个数据集

- 真实格式、编码 / 分隔符、字段类型、缺失值、评分范围和显式 / 隐式语义。
- 重复评分的时间戳与取舍政策、书目 ID 合并政策，以及稳定 ID 映射发布 / 增量策略。
- 数据许可、实际来源及资源规模；真实元数据、作者和可选标签的适配。
- 新训练 / 验证 / 测试划分、正样本口径、ALS 参数、模型来源绑定及独立评价计划。
- 实数量表的用户画像阈值 / 分布，受控运行时数据集注册、推荐工厂、WebUI 来源与缓存同步切换。

本阶段不改变正式指标、不训练 Goodbooks ALS、不写生产 HDFS、不改集群或网络配置，也不修改任何原始演示 JSON 或运行时演示清单。
