# Spark 图书推荐与阅读偏好分析

基于 Hadoop HDFS、Spark SQL、MLlib ALS 和 Streamlit 的 Goodbooks-10k 课程项目。已实现评分检查、Parquet、统计图表、用户评分画像、主题和作者偏好、ALS / Popularity 推荐及未知用户热门回退。

偏好依据是评分历史，不代表用户实际读完图书。标签用于偏好解释，未作为新特征重训 ALS。封版核验日期为 **2026-10-28**，当前分支为 `feature/user-preferences`。

当前 GitHub 仓库为 **Private**，访问源码需要获授权的 GitHub 账号。2026-10-28 认证连接器确认可访问；加固开始前本地 HEAD、origin 跟踪引用及远端该分支均为 `a8f3c78`，此前页面与封版材料已提交。本次加固仅保留为工作区改动，未执行 commit、push 或 merge。历史审计记录中的 `eee13c5` 和 404 是当时的检查结果，当前状态见 [资源清单](docs/project_resources.md)。

## 数据与评价摘要

| 项目 | 已验证结果 | 范围 |
| --- | --- | --- |
| 评分 / 用户 / 图书 | 5,976,479 / 53,424 / 10,000 | 原始 CSV 全量只读检查 |
| 通用 / 主题标签关系 | 999,898 / 114,662 | 第一阶段审计，均覆盖 10,000 本书 |
| 正式训练 / 保留测试评分 | 5,408,526 / 567,953 | 原确定性哈希划分 |
| ALS / 图书均分 RMSE | 0.836010 / 0.953327 | 冻结模型复测；567,953 条相同有效测试记录 |
| ALS / Popularity Recall@10 | 0.009142 / 0.066077 | 确定性选择的 100 名合格测试用户 |
| ALS / Popularity HitRate@10 | 0.08 / 0.32 | 同上；测试评分 ≥4 为正样本 |

ALS 评分预测 RMSE 更低，Popularity 在本次 Top-K 样本中更好。不能将 RMSE 改善解释为排序效果全面改善，也不能把 100 人结果推广到所有用户。调参每组历史验证 RMSE 未找到归档，未补造或重训。完整口径见 [实验结果](docs/experiment_results.md)。

## 环境与资源边界

| 位置 | 实测环境 |
| --- | --- |
| Ubuntu 主机 | Ubuntu 24.04.5 LTS；Python 3.12.3；项目 `.venv` |
| WebUI | Streamlit 1.66.1；pandas 3.1.0；本机无需 PySpark |
| Docker | Engine 29.8.1；Compose 5.5.1；bigdata/spark-bookrec:3.5.5 |
| 容器计算 | Spark 3.5.5；Hadoop 3.3.6；OpenJDK 11.0.32.1 |
| 地址 | spark://master:7077；hdfs://master:9000/user/root/bookrec |

[requirements-lock.txt](requirements-lock.txt) 固定本机全部 44 个 pip 包，包括可选绘图 / 浏览器测试依赖；[requirements-web.txt](requirements-web.txt) 是精简入口。操作系统共享库不包含在 pip 锁文件中。

**代码仓库不是完整环境镜像。** 运行依赖仓库外的 `~/lab04/compose.yaml`、教学镜像、已有 Docker 网络及三个 HDFS 数据卷。模型和 Parquet 在本地 HDFS，尚无公开下载地址。见 [项目资源清单](docs/project_resources.md)。

## 在现有虚拟机恢复运行

以下假定镜像、网络、数据卷和 HDFS 产物已存在，不是创建空集群的命令：

```bash
cd /home/bigdata/spark-book-recommender
# Docker 未运行时由操作者执行：sudo systemctl start docker
# 仅在虚拟机重启或集群已停止时运行，正常演示中不重复运行：
bash infra/start-bookrec.sh

# 沿用已有 .venv；全新主机才先创建 python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-lock.txt
# outputs 缺资源时，先按复现指南从 docs/artifacts 恢复
.venv/bin/python scripts/prepare_webui_demo.py
bash scripts/start_webui.sh
```

本机访问 <http://127.0.0.1:8501>；Windows 使用 `http://<Ubuntu虚拟机IP>:8501`。本次为 <http://192.168.142.137:8501>，2026-10-27 用户确认 Windows 可访问，2026-10-28 恢复页面后本机及局域网健康检查为 200。

当前仍监听 `0.0.0.0:8501`。建议保留 Windows 的 VMware NAT 访问方式，通过 Ubuntu 入站规则只允许本机和 Windows VMnet8 实际源地址访问 8501；[限制方案及验证 / 回退步骤](docs/webui.md#8501-访问限制方案仅报告未应用)已记录。本次没有改监听、防火墙或 VMware 配置，没有重启 WebUI。

Streamlit 在前台运行，需保持终端打开；未设置系统自启。Docker 服务为 enabled，但容器 restart policy 为 no，重启后仍需恢复集群。详细顺序、资源还原、无停机验证及初始化边界见 [复现指南](docs/reproducibility.md)。本轮未实际重启虚拟机、恢复卷或重建容器。

## 使用

```bash
# 默认 ALS；追加 --mode popularity 切换热门模式
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  src/recommend.py --user-id 660 --top-k 10

docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 \
  src/analyze_preferences.py --user-id 660 --top-k 10
```

WebUI 的三个区域为概览、偏好、推荐。660、1000、2000 的历史画像及已归档推荐可直接演示；新用户需明确启用按需计算。页面分别展示请求算法和实际算法，999999 请求 ALS 会回退至 Popularity。ALS 原始预测可能超出 1～5；Popularity 得分是训练集评分次数。

本机读取展示资源；必要时通过参数数组调用容器 Spark，在 bind mount 独立 JSON 中交换结果，不解析日志。按需缓存默认 1 小时，数据 / 模型变化需更新版本。详见 [WebUI](docs/webui.md)、[推荐](docs/recommendation_modes.md)、[偏好](docs/user_preferences.md)、[标签](docs/tag_features.md)。

## 文件与模块总览

| 模块 | 职责 |
| --- | --- |
| src/inspect_data.py、prepare_data.py | 数据质量检查、评分与图书 Parquet |
| src/analyze.py、visualize.py | Spark SQL 统计、Matplotlib 图表 |
| src/train_als.py、tune_als.py、train_final.py | 历史训练 / 调参；封版复查不执行 |
| src/evaluate_topk.py | 已有模型的 100 用户 Top-K 评价 |
| src/prepare_tags.py | Goodreads ID 映射、去重与两套标签特征 |
| src/preferences.py、analyze_preferences.py | 画像计算与 JSON CLI |
| src/recommenders.py、recommend.py | 可复用双模式推荐、兼容 CLI |
| src/web_backend.py、web_job.py、app.py | 本机缓存、安全桥接与页面 |
| infra/ | 既有镜像定义、Compose 覆盖配置及恢复脚本 |
| scripts/、tests/ | 检查、验收、页面启动、覆盖保护与业务测试 |
| docs/artifacts/ | 可提交的真实指标、演示快照、图表、截图与校验和 |
| data/、models/、outputs/、.venv/ | 数据、导出、运行产物、环境，不提交 Git |

## 验证与交付

```bash
.venv/bin/python -m unittest discover -s tests -p test_output_safety.py -v
# 仅在容器 /tmp 创建临时测试数据，空 PipelineModel 不执行 fit：
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit --master 'local[1]' tests/test_output_safety_spark.py
.venv/bin/python -m unittest discover -s tests -p test_preferences.py -v
.venv/bin/python -m unittest discover -s tests -p 'test_web*.py' -v
.venv/bin/python -m pip check
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit --master 'local[2]' tests/test_prepare_tags.py
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit --master 'local[2]' tests/test_recommenders.py
```

本次加固：覆盖保护 14 项、临时 Spark 写入 2 项、偏好 11 项、WebUI / 后端 14 项均通过，共 41 项；未重跑标签 / 推荐 Spark 测试或正式评价。历史封版的 5+11+11+14=41 项记录仍原样保留。只读复测命令和副作用入口见 [复现指南](docs/reproducibility.md#覆盖写入保护)。

`prepare_data.py`、`train_als.py`、`train_final.py` 保留原 spark-submit 入口与输出路径，默认在读取数据 / 训练前拒绝任何已有目标；只有完整的 `--overwrite` 参数才授权替换该脚本的输出。模型保存默认不调用 overwrite，Parquet 使用 errorifexists，再次防止误覆盖。无参数且目标不存在时仍会正常准备数据或训练；`--overwrite` 也会实际运行原流程，不能用于只读验证。本次未执行这些业务入口。

- [历史封版验收、当时的 Git 提交建议与 PPT 选图](docs/release_checklist.md)（审计起点 eee13c5；当前提交状态见资源清单）
- [实验结果与评价口径](docs/experiment_results.md)
- [资源公开性与模型导出方案](docs/project_resources.md)
- [交付快照及来源](docs/artifacts/README.md)

数据源为 [zygmuntz/goodbooks-10k](https://github.com/zygmuntz/goodbooks-10k)，上游 [LICENSE](https://github.com/zygmuntz/goodbooks-10k/blob/master/LICENSE) 声明 CC BY-SA 4.0。代码尚无独立 LICENSE。项目源码位于 [Private 仓库 AliceChennnn/spark-book-recommender](https://github.com/AliceChennnn/spark-book-recommender)，不是公共下载链接或可供未授权访问的源码二维码。
