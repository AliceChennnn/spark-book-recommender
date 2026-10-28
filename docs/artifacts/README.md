# 精选交付快照

此目录是可以随源码审阅提交的小型真实材料，不是整个运行缓存的例外放行。原始 CSV、Parquet、因子权重、完整日志、浏览器二进制和虚拟环境仍不进入 Git。

| 目录 / 文件 | 内容与来源 |
| --- | --- |
| demo/preferences | 660、1000、2000 的真实评分画像，2026-10-27 |
| demo/recommendations | 660、1000、2000、661、999999 的两模式结果，2026-10-27 |
| statistics | 原有四份 Spark SQL CSV |
| figures | 三张原统计图；2026-10-28 只读复测生成的 RMSE / Top-K 对比图 |
| screenshots | 前阶段真实 Chromium 页面截图；未编辑图中结果 |
| metrics | 标签、画像比较、推荐 / 页面验收及本轮只读指标证据 |
| data-checksums.json | 本地四份原始 CSV 和上游 LICENSE 的 SHA-256，未打包数据本体 |
| provenance.json | 从原 outputs / HDFS 复制的来源对应关系 |
| manifest.json | 精选材料的逐文件字节和 SHA-256，用于交付后完整性核验 |
| DATA_LICENSE.txt | Goodbooks-10k 上游原始许可说明 |

数据来源：[Goodbooks-10k](https://github.com/zygmuntz/goodbooks-10k)，上游声明 CC BY-SA 4.0。保留来源和许可；项目代码许可尚未指定。图表 / 画像反映数据集已有评分和群体标签，非真实读完记录，也不是线上推荐效果实验。

source_commit 记录为 eee13c5，是封版审计起点。第四阶段页面文件和本轮文档仍在工作区，所以此值不是声称已提交完整最终交付的 tag。提交后由作者记录最终 commit，不在本阶段自动打 tag 或推送。

复测结果日期为 2026-10-28，其他历史快照为 2026-10-27，具体见 experiment_results.md。统计图原始结果生成于更早阶段，本轮复制，不重跑覆盖。

首次克隆后恢复展示资源的方法见 reproducibility.md。复制快照只能恢复既定版本演示，不能代替 HDFS、模型或新的按需结果。更换数据版本后不得继续冒用旧快照。

manifest 不对自身计算哈希；它记录生成时除自身以外的全部交付材料。目录内容经作者修改后应重新生成或更新校验清单，避免继续声称摘要匹配。
