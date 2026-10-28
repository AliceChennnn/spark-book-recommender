# 项目资源与可公开访问性

检查日期：2026-10-28。已核对本地 Git、未登录 GitHub API、连接器及 HDFS，不将私网地址当作公共下载链接。

## 资源清单

| 资源 | 位置 | 当前可访问性 / 交付状态 |
| --- | --- | --- |
| 项目 GitHub | https://github.com/AliceChennnn/spark-book-recommender | 未登录 API 404，连接器也 404；无法证明公开，不能判断一定不存在 |
| 本地 Git | feature/user-preferences，HEAD eee13c5 | 相对本地 origin/feature 引用领先 1；未提交第四 / 五阶段文件仍在工作区 |
| 远端 feature 引用 | 本地缓存为 e0d9090 | git ls-remote 因缺认证失败，未 fetch，无法核验真实远端最新状态 |
| Goodbooks-10k | https://github.com/zygmuntz/goodbooks-10k | 未登录 API 200，visibility=public，网页公开可读 |
| 数据许可证 | 上游 LICENSE；artifacts/DATA_LICENSE.txt | CC BY-SA 4.0，已附来源；项目代码没有独立 LICENSE |
| 原始 CSV / zip | data/raw；HDFS raw | 本地被忽略；数据可从公开上游另取，本次内容有 SHA-256 |
| processed Parquet / 标签 | HDFS processed | 本地课程环境可读，未提供公共下载包 |
| ALS 权重 | HDFS models/als_full_v1 | 只在当前 HDFS 中，未导出权重 / 上传发布 |
| 模型元数据 | artifacts/metrics/model-metadata.json | 已复制小型 rank / schema 元数据，不是可加载模型权重 |
| 本机 Python 依赖 | requirements-lock.txt | 44 包的当前环境快照；需另备 OS 共享库 |
| 本地镜像 | bigdata/spark-bookrec:3.5.5 | 可 inspect，约 5.05 GB；未验证任何公共 registry 下载 |
| 教学离线归档 | ~/lab04/bigdata-spark-3.5.5-hadoop3.3.6-amd64.tar.gz | 当前约 1.7 GB，本地存在，非项目 Git 交付件；未执行导入验证其 tag 覆盖 |
| 基础 Compose | ~/lab04/compose.yaml | 仓库外，1135 字节；与 infra 覆盖文件合并校验通过 |
| HDFS 数据卷 | lab02-student_hadoop-*-data 三卷 | 当前存在，不在 Git；未备份 / 恢复演练 |
| 图表 / 演示 / 截图 | docs/artifacts | 已精选复制，可随本次源码提交；不是运行缓存 |
| WebUI | http://192.168.142.137:8501 | 本地课程网地址，非已发布公网服务；重启后 IP 可能改变 |

公开来源：[Goodbooks-10k](https://github.com/zygmuntz/goodbooks-10k)、[上游 LICENSE](https://github.com/zygmuntz/goodbooks-10k/blob/master/LICENSE)。项目链接 404 可能是私有、权限或路径问题，需作者在未登录浏览器 / 无 token 的 API 环境确认后，才将其放为 PPT 公共源码二维码。

## 模块依赖

```mermaid
flowchart LR
  R[HDFS 原始 CSV] --> P[评分 / 图书 Parquet]
  R --> T[标签 ID 映射与主题权重]
  P --> S[SQL 统计与图表]
  P --> A[已有 ALS 模型]
  P --> H[评分历史偏好]
  T --> H
  P --> B[训练集 Popularity]
  A --> C[BookRecommender]
  B --> C
  H --> J[独立 JSON 桥接与缓存]
  C --> J
  S --> W[本机 Streamlit]
  J --> W
```

图中的模型已在前期训练，本次不重训；标签只用于评分偏好解释，不声称增强了 ALS 模型。详尽文件列表见 README 与 release_checklist。

## 绝对路径、环境与演示代码审查

| 项目 | 发现与处理 |
| --- | --- |
| /home/bigdata/spark-book-recommender | 现有 bind mount 的主机路径；不为本轮改动集群配置 |
| /workspace/bookrec、/usr/local/spark、/usr/bin/python3 | 当前容器接口约定，已在文档说明 |
| hdfs://master:9000、spark://master:7077 | 课程 Docker 网络内地址，不能当外部浏览器链接 |
| HOME/lab04 | 启动必须的仓库外依赖，不是可省略的临时变量 |
| BOOKREC_DATA_VERSION | 页面有默认值，仅换数据 / 模型时设置并重启 |
| /tmp 浏览器库 / 字体路径 | 仅可选自动化工具，无主程序依赖；清空后需重建测试环境 |
| 660、1000、2000、661、999999 | 默认输入、演示快照白名单和验收样例；真实值由算法 / JSON 读取，不是硬编码推荐输出 |
| tests 中 FixedALSModel | 明确的测试桩，不是正式模型或真实实验结果 |
| 代理地址、调试断点、凭据 | 本轮源码 / 脚本 / 文档检索未发现代理配置、breakpoint / set_trace 或显式访问密钥；未读取或输出主机私密凭据 |

启动脚本未要求导出临时代理。宿主机原有用户网络环境不在项目配置范围，不能把未清空环境的检查称为所有网络场景验证。

## ALS 原生模型资源

路径为 hdfs://master:9000/user/root/bookrec/models/als_full_v1，存在 metadata、userFactors、itemFactors 三部分；Spark 3.5.5、rank=20。HDFS 逻辑大小 5,345,361 字节，含双副本的计费字节为 10,690,722。本次未将因子表导出到 CSV，也未修改保存模型。

仅 metadata JSON 不足以 ALSModel.load。可复现推荐还需要一致的内部 book_id 映射及完整评分历史用于已评分排除；可复现评价还需要相同评分 Parquet、INT 哈希类型和划分规则。

## 安全导出方案（仅方案，未执行）

1. 约定唯一的 outputs/exports/<批次> 目录，先检查目标不存在，记录源模型路径、Spark 版本、模型元数据、train_final.py SHA-256 和数据 CSV 校验和。
2. 只读使用 hdfs dfs -get，将完整 als_full_v1 复制到 bind mount 的新目录；不使用 -f、mv、rm 或模型 write，不直接操作 HDFS 数据卷。
3. 对复制件逐文件生成 SHA-256 清单，核对 metadata / userFactors / itemFactors 的文件数、字节及成功标志；记录导出前后源 HDFS 检查信息。
4. 在复制件上创建唯一命名 tar.gz。打包内容保留 Spark 原生目录结构、数据映射 / 版本说明与上游许可，不仅打包因子向量。
5. 操作者同意验证复制件时，用独立本地路径 ALSModel.load，只做推理并比较 660 推荐，与源模型对比；绝不 fit。
6. 对最终压缩包生成 SHA-256，明确访问权限 / 载入方法，再由作者决定是否上传 release 或课程交付盘。当前没有模型下载 URL。

示例只读复制命令，需先建好且保证目录为新批次，本次没有执行：

```bash
docker exec master hdfs dfs -get /user/root/bookrec/models/als_full_v1 \
  /workspace/bookrec/outputs/exports/<唯一批次>/als_full_v1
```

若要交付整套 HDFS 卷 / 虚拟机，应另安排一致性快照或停机窗口；不能在运行中随意压缩部分卷就声称可恢复。本阶段不执行会中断页面的方案。

## 尚未解决的交付条件

- GitHub 可见性与认证未解决，最新源码尚未提交 / 推送。
- 模型、Parquet、教学镜像、基础 Compose 和数据卷尚未形成可公开下载的完整环境包。
- 未实际进行虚拟机重启 / 卷恢复演练；环境自启仅有人工流程。
- 调参每组历史验证日志缺失；本轮只补齐冻结模型 RMSE 与原 Top-K 只读复测。
- 代码许可尚未选择；数据来源与许可已附，不自行为作者决定代码授权。

这些是交付范围限制，不影响当前虚拟机上的已验证业务流程。
