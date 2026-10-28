# 复现与恢复指南

核验日期：2026-10-28。区分已有虚拟机恢复、只读指标复测和新机器部署。未执行的停机演练不写成已验证操作。

## 必须具备的资源

固定主机目录为 `/home/bigdata/spark-book-recommender`，容器目录为 `/workspace/bookrec`。现有 Compose bind mount 和页面桥接均使用这些路径；本轮不为迁移改动集群配置。

必须已有 Docker 访问权限、bigdata/spark-bookrec:3.5.5 镜像、`~/lab04/compose.yaml`、lab02-student_hadoopnet 网络，以及下列三个一致的数据卷：

```text
lab02-student_hadoop-master-data
lab02-student_hadoop-worker1-data
lab02-student_hadoop-worker2-data
```

HDFS 应已有 raw 四份 CSV、processed/ratings、processed/books、processed/tag_features_v1、models/als_full_v1。不能创建同名空卷代替数据恢复，也不能只复制一个 DataNode 卷声称完整恢复。本阶段不格式化、不删卷、不重训。

## 虚拟机重启后的人工恢复顺序

先只读检查；正常演示中不反复执行 up 或启动守护进程：

```bash
cd /home/bigdata/spark-book-recommender
systemctl is-active docker
docker ps -a
docker image inspect bigdata/spark-bookrec:3.5.5
docker network inspect lab02-student_hadoopnet
docker volume inspect lab02-student_hadoop-master-data \
  lab02-student_hadoop-worker1-data lab02-student_hadoop-worker2-data
test -f "$HOME/lab04/compose.yaml"
```

Docker 未运行时由操作者执行 `sudo systemctl start docker`。本次服务为 enabled，但三个容器 restart policy 为 no，开机不等于集群已恢复。

确认现有卷、镜像完好，且集群已停止后执行：

```bash
bash infra/start-bookrec.sh
```

脚本检查镜像、网络、卷及 NameNode VERSION，合并外部 Compose 与项目覆盖文件，依次等待 SSH、HDFS / YARN、Spark。不会自动格式化或创建替代空卷。使用普通 HOME 定位 lab04，没有临时代理变量要求。

恢复后核验：

```bash
docker exec master hdfs dfsadmin -report
docker exec master yarn node -list
docker exec master jps
docker exec worker1 jps
docker exec worker2 jps
docker exec master hdfs dfs -ls /user/root/bookrec/processed /user/root/bookrec/models
```

本次：2 个 Live DataNodes，0 缺失 / 损坏 / 欠副本块，YARN 报告 2 个 RUNNING 节点；Spark Master 与三个 Worker 进程存在。限制总核数为 2 的 RDD 验证实际运行于 master、worker1，不据此宣称覆盖了所有 Worker 的任务调度。

## 本机依赖和首次克隆的展示资源

使用已有 .venv；新环境才先运行 `python3 -m venv .venv`，无需删除当前环境。

```bash
.venv/bin/python -m pip install -r requirements-lock.txt
.venv/bin/python -m pip check
```

锁文件记录本次 Ubuntu / Python 的全部 44 个 pip 包，含 Matplotlib、可选 Playwright；不包含镜像和 OS 共享库。requirements-web.txt 是精简入口，其传递依赖未逐项锁定。

Git 不含 outputs 等运行目录，可从仓库内真实小型快照恢复展示资源，cp -n 保留已有本地产物：

```bash
mkdir -p outputs/analysis outputs/figures outputs/user_preferences_v1 outputs/recommendation_modes_v1
cp -n docs/artifacts/statistics/*.csv outputs/analysis/
cp -n docs/artifacts/figures/rating_distribution.png outputs/figures/
cp -n docs/artifacts/figures/popular_books.png outputs/figures/
cp -n docs/artifacts/figures/user_activity.png outputs/figures/
cp -n docs/artifacts/demo/preferences/*.json outputs/user_preferences_v1/
cp -n docs/artifacts/demo/recommendations/*.json outputs/recommendation_modes_v1/
.venv/bin/python scripts/prepare_webui_demo.py
```

快照不能替代新模型结果。代码 / 数据版本不一致时先重算对应快照，再注册清单，不要将旧数据重新标为新版本。

本地图书总数还依赖 data/raw/books.csv。由 Goodbooks-10k 官方仓库或 release 获取 ratings.csv、books.csv、book_tags.csv、tags.csv，放入 data/raw 并保留 LICENSE。使用 docs/artifacts/data-checksums.json 校验本次数据；上游 master 可能变化，不能假设永远相同。本阶段没有声称已锁定上游某个 Git 提交。

仅缺本地 books.csv 时页面提示图书总数暂无，不伪造数字，缓存结果仍可展示；缺 Docker / HDFS 时仅能演示快照，无法按需计算。完整新机器部署还缺镜像和 HDFS 交付包，见资源清单，不能只靠 clone 完成。

## 页面启动与环境变量

```bash
bash scripts/start_webui.sh
```

前台运行需保持终端打开，未设置 systemd 自启。需要脱离终端时，操作者先确认 8501 空闲，再执行：

```bash
mkdir -p outputs/webui
nohup bash scripts/start_webui.sh > outputs/webui/server.log 2>&1 < /dev/null &
```

本轮检查时 WebUI 未监听，已使用同一脚本在独立会话恢复；PID / 日志位于 outputs/release_audit_20261028。未停止或重启任何集群服务。

访问 http://127.0.0.1:8501 或 http://<Ubuntu虚拟机IP>:8501，本次为 192.168.142.137。重启后使用 hostname -I 重新确认地址。健康检查路径为 /_stcore/health，本次本机及局域网均返回 200。Windows 访问已于 2026-10-27 由用户确认，本阶段未要求再次跨系统测试。

正常启动不依赖 /tmp 中的 FONTCONFIG_FILE、LD_LIBRARY_PATH、PLAYWRIGHT_BROWSERS_PATH。BOOKREC_DATA_VERSION 有默认值，仅数据 / 模型改变时显式设置并重启页面。临时图形库和字体只影响可选浏览器自动化测试，不是页面运行依赖。

## 无停机验证命令

以下本地测试运行于独立进程，不清空在线页面会话：

```bash
.venv/bin/python -m unittest discover -s tests -p test_preferences.py -v
.venv/bin/python -m unittest discover -s tests -p 'test_web*.py' -v
.venv/bin/python -m pip check
bash -n scripts/start_webui.sh infra/start-bookrec.sh
docker compose -f "$HOME/lab04/compose.yaml" -f infra/compose.bookrec.yaml config --quiet
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit --master 'local[2]' tests/test_prepare_tags.py
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit --master 'local[2]' tests/test_recommenders.py
```

以上是历史封版的 41 项测试入口。推荐单元测试使用固定预测桩，不将其数值作为真实 ALS 指标。本次覆盖保护的专项测试见下文，未重跑标签 / 推荐 Spark 测试或正式指标。

现有集群的真实只读检查：

```bash
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 scripts/check_cluster.py
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 src/inspect_data.py
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 src/evaluate_topk.py
```

RMSE 不通过 train_final.py 复查：目标不存在或显式授权覆盖时，该脚本仍会 fit 并保存模型；默认会先拒绝已有目标。只读审计脚本使用已有 ALSModel.transform 和 RegressionEvaluator：

```bash
mkdir -p outputs/audit-new
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master spark://master:7077 --total-executor-cores 2 scripts/audit_frozen_model.py \
  --output-json /workspace/bookrec/outputs/audit-new/frozen-model-rmse.json
```

目标必须不存在，重跑用新路径。真实双模式验收可用同一 spark-submit 前缀运行 `scripts/validate_recommenders.py --output-dir outputs/recommendation-audit-new`。页面桥接验收为 `.venv/bin/python scripts/validate_webui.py --report outputs/webui/validation-new.json`；缓存失效时可能按需计算，不会训练。

可选 Chromium 测试的共享库 / 字体要求见 webui.md。本轮 41 项测试通过不代表在清空 /tmp 的机器上重新验证了 Chromium 安装；页面本身不依赖它。

## 覆盖写入保护

三个入口保留原有 spark-submit 用法、路径、训练参数与数据划分，新增 `--overwrite`（默认为 false，不接受缩写或 `--overwrite=false`）。`--help` 和模块导入不会创建 SparkSession，可在没有 PySpark 的本机查看帮助：

```bash
.venv/bin/python src/prepare_data.py --help
.venv/bin/python src/train_als.py --help
.venv/bin/python src/train_final.py --help
```

| 入口 | 固定输出（相对 hdfs://master:9000/user/root/bookrec） |
| --- | --- |
| prepare_data.py | processed/ratings 和 processed/books；任一已存在则默认全部拒绝，读取数据和首个写入前检查两个路径 |
| train_als.py | models/als_pilot；已有模型则在读取评分和 fit 前拒绝 |
| train_final.py | models/als_full_v1；已有模型则在读取评分和 fit 前拒绝 |

检查使用目标 URI 自己的 Hadoop FileSystem，连接 / 权限错误直接失败，不当作“目标不存在”。保存时模型使用默认 MLWriter，Parquet 使用 errorifexists；即使前置检查后出现同名目标，写入器仍拒绝替换。只有给上述原命令的脚本参数追加完整的 `--overwrite` 才允许该入口覆盖自己的固定目标；prepare_data.py 的授权作用于两个输出。它不是试运行开关，本次没有对生产目标使用。

保护不提供跨路径事务或多进程锁：第二个 Parquet 写入失败时可能留下第一个新输出，真正同时执行的作业仍应避免；显式覆盖时也可能发生部分替换，不会自动备份或回滚。失败重试默认拒绝已有的完整 / 部分目录。只读核验应继续使用审计脚本，不运行训练入口；标签入口仍保持已有的新目录保护策略。

新增验证命令只操作测试桩或新建的容器 file:///tmp 临时目录：

```bash
.venv/bin/python -m unittest discover -s tests -p test_output_safety.py -v
docker exec -w /workspace/bookrec master /usr/local/spark/bin/spark-submit \
  --master 'local[1]' tests/test_output_safety_spark.py
```

2026-10-28：14 项纯 Python 保护测试 + 2 项实际 Spark 临时写入测试通过；后者使用空 PipelineModel 验证模型保存，未执行任何 fit。覆盖默认拒绝、完整参数授权、文件系统异常、第二目标冲突、写入时再次拒绝及异常后停止本次 SparkSession。另有偏好 11 项、后端 / WebUI 14 项回归通过，共 41 项。本次未执行正式训练、冻结模型复测或 Top-K 实验。

前后检查中，HDFS processed / models 的 444 行递归目录清单完全一致，61 份历史 artifacts / 本地演示文件 SHA-256 未变；三个脚本的原计算部分也通过 AST 比对（排除入口和保存策略）。WebUI 保持同一进程，loopback / NAT 地址健康检查均为 HTTP 200；本次未从 Windows 再做跨系统浏览器验证。

## 本次未执行的业务副作用操作

| 入口 | 副作用与状态 |
| --- | --- |
| prepare_data.py | 默认拒绝已有输出，仅 --overwrite 授权替换 processed/ratings、processed/books；未执行 |
| analyze.py、visualize.py | 覆盖统计 / 图片；使用已有结果，未执行 |
| train_als.py、train_final.py | 会 fit，已有模型默认拒绝，--overwrite 授权替换；未执行 |
| tune_als.py | 调参会 fit，不在本次加固范围；未执行 |
| prepare_tags.py | 写新目录，已有路径拒绝；本轮仅读原审计 |
| 虚拟机重启、容器重建、卷备份恢复 | 可能中断服务；未执行 |
| 模型权重 / 数据卷导出 | 仅提供方案，未执行 |

真实重启恢复需要另约停机窗口，不通过删除 / 重建卷模拟故障。
