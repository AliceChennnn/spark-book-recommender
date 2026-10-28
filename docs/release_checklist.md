# 封版验收与课程材料

日期：2026-10-28。审计起点 HEAD eee13c5，分支 feature/user-preferences。相对本地 origin 引用领先 1；未联网更新引用。第四阶段页面文件及本轮文档 / 审计资源仍需作者审阅提交，代理未执行 add、commit、push 或 merge。

## 功能验收表

| 项目 | 结果 | 范围与证据 |
| --- | --- | --- |
| 全量评分质量 | 通过 | 本轮 inspect_data.py；关键空值 / 非法分 / 重复 / 失配均 0 |
| 评分 / 图书 Parquet | 已存在、只读复核 | 行数与逻辑大小；未重做覆盖预处理 |
| 标签清洗与归一化 | 通过 | 前期 HDFS 审计快照 + 本轮 5 项测试 |
| 用户偏好 | 通过 | 3 用户真实快照 + 本轮 11 项逻辑测试 |
| ALS / Popularity / 回退 | 通过 | 前期 5 用户 10 请求；本轮 11 项 Spark 测试 |
| 保留测试历史排除 | 通过 | 661 的 10 条保留记录已排除；离线评价口径另列 |
| 旧 CLI 兼容性 | 通过 | 默认 ALS、Top-K 顺序与分数匹配；前阶段真实记录 |
| 页面 / 适配层 | 通过 | 本轮 14 项测试；前期真实 Chromium / Windows 证明 |
| 只读 RMSE / Top-K | 通过复测 | 本轮冻结模型，无训练；完整数值见 experiment_results |
| 当前集群和页面健康 | 通过 | HDFS / YARN / Spark、Sum=500500、RDD=1000、HTTP 200 |
| 精选快照隔离恢复 | 通过 | 临时目录恢复 13 份 JSON、概览及 3 张图，0 次 Spark；不等于 HDFS 恢复 |
| 虚拟机重启 / 卷恢复 | 未演练 | 不为本次封版中断服务 |
| 全新主机仅 clone 即运行 | 不满足 | 缺环境 / 模型资源交付与公开仓库核验 |

41 项测试 = 标签 5 + 偏好 11 + 推荐 11 + WebUI / 后端 14。没有把多个 shell 命令、HTTP 访问和前期浏览器验证混进此计数。

## 本轮审查与改动

- .gitignore：补齐测试工具缓存、凭据、临时文件、PID、模型 / 镜像归档和 Parquet 规则；没有忽略源码、指标 JSON 和精选文档图片。
- README：从计划功能改为实际功能、环境、资源边界、启动和测试入口。
- requirements-lock.txt：冻结现有 44 个包，不重新安装或变更业务依赖。
- scripts/audit_frozen_model.py：只读加载冻结模型计算原口径 RMSE，避免误运行会 fit 的 train_final.py。
- docs/reproducibility.md：恢复顺序、离线快照还原、环境变量、已验证 / 未演练命令。
- docs/experiment_results.md：有范围与来源的指标，含 Popularity 排序优于 ALS 的实际结果。
- docs/project_resources.md：公开性、外部依赖、路径审查和未执行的权重导出方案。
- docs/artifacts：真实小型快照、校验和、指标图与截图，补足 outputs 被忽略导致交付无图的问题。

第四阶段待提交内容清单：app.py；requirements-web.txt、requirements-web-test.txt；src/web_backend.py、src/web_job.py；scripts/prepare_webui_demo.py、start_webui.sh、validate_webui.py、test_webui_browser.py；tests/test_web_backend.py、test_webui.py；docs/webui.md。前几阶段源码和测试已跟踪，无需大规模移动或重构。

完整已跟踪、未跟踪、已忽略的逐文件清单保存在本地 outputs/release_audit_20261028/git-inventory.json，不提交庞大的 .venv 文件名列表。精选审计摘要随文档归档。没有已跟踪文件被忽略规则误覆盖，没有把 raw / model / logs / cache 改为 Git 追踪。

## 建议 Git 命令（由作者执行，本次未执行）

```bash
git branch --show-current
git status --short --branch
git diff --check
git diff -- README.md .gitignore
```

确认当前为 feature/user-preferences，审查新增文件后先预览：

```bash
git add --dry-run .gitignore README.md requirements-lock.txt \
  app.py requirements-web.txt requirements-web-test.txt \
  src/web_backend.py src/web_job.py \
  scripts/prepare_webui_demo.py scripts/start_webui.sh \
  scripts/validate_webui.py scripts/test_webui_browser.py scripts/audit_frozen_model.py \
  tests/test_web_backend.py tests/test_webui.py docs/
```

预览无数据、模型、虚拟环境、日志或临时缓存后，作者可用相同文件列表执行 git add，并检查 `git diff --cached --stat`、`git diff --cached --name-only`。不要 git add -f outputs 或 data。可按“WebUI 与验证”“封版文档与实测证据”分成两次提交，或统一一条：

```bash
git commit -m "docs: finalize reproducibility and audited delivery artifacts"
# 仅在作者确认远端权限及推送范围后：
git push origin feature/user-preferences
```

不建议将未审阅结果直接合并 main。当前远端公开性无法确认，以上推送命令不是已经执行的结果。

## 最终 PPT / PDF 建议保留的图

| 材料 | 文件 | 建议说明 |
| --- | --- | --- |
| 数据概览页面 | artifacts/screenshots/overview.png | 数据规模和真实页面；不是公网部署证明 |
| 评分分布 | artifacts/figures/rating_distribution.png | 全量 5,976,479 条，1～5 分偏正向 |
| 热门榜 / 活跃度 | artifacts/figures/popular_books.png、user_activity.png | 评分次数热度，不是原始 Goodreads ratings_count |
| 画像页面 | artifacts/screenshots/preferences.png | 660 的评分画像；其他用户表格取真实 JSON |
| RMSE 对比 | artifacts/figures/rmse_comparison.png | 567,953 有效测试记录，ALS 相对降低 12.31% |
| Top-K 对比 | artifacts/figures/topk_comparison.png | 100 人宏平均，Popularity 两项更好，必须保留该结论 |
| 冷启动页面 | artifacts/screenshots/fallback.png | 999999：请求 ALS / 实际 Popularity，计数分数 |
| 数据与模型资源 | project_resources.md 清单 | 数据公开；源码公开性未证实，权重尚未发布 |

讲述顺序可为问题与数据、架构 / ID 映射、清洗与统计、ALS 配置、两类评价、画像解释、页面演示、局限和资源。图表配上采样人数、正样本阈值、训练 / 测试边界、日期和数据来源。

不要制作缺失日志的调参 RMSE 曲线，不把 100 人称作全量用户，不将低 RMSE 推导为更好的推荐排序，不将书架标签称作用户真实阅读完成或客观兴趣概率。未重新测量的均值延迟、P95、并发吞吐或加速倍数不进入 PPT。

本阶段提供可追溯素材与说明，未自动生成或发布最终 PPT / PDF。
