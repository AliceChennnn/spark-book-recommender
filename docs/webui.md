# Streamlit WebUI

## 实际运行架构

Ubuntu 本机 Python 3.12 虚拟环境运行 Streamlit 1.66.1，监听 `0.0.0.0:8501`。本机无需 PySpark，不在页面进程中加载容器内的模型。

```text
Windows / Ubuntu 浏览器 → Ubuntu Streamlit
                           ├→ 本地 CSV、PNG、已有演示 JSON
                           ├→ 按请求缓存 JSON
                           └→ 参数数组 subprocess.run
                                 → docker exec master timeout spark-submit
                                 → web_job.py 调用已有偏好函数 / BookRecommender
                                 → bind mount 独立 result.json → Ubuntu 读取并校验
```

容器原有 `/home/bigdata/spark-book-recommender:/workspace/bookrec` 挂载保持不变。Spark 3.5.5 由 spark-submit 设置 PySpark 路径；容器中直接执行普通 python3 无法 import pyspark 不影响此方式。原有两个 CLI 的 JSON 写到 stdout，Spark 日志另有输出通道；页面新增独立桥接入口 src/web_job.py，使用独立 JSON 文件交换，完全不解析混合终端输出。

每次未缓存请求启动一个短期 Spark 应用，不训练模型；同请求缓存命中不启动应用。网页不重新实现 ALS、热度统计或偏好算法。

## 安装与启动

在项目根目录使用已有 .venv：

```bash
.venv/bin/python -m pip install -r requirements-web.txt
.venv/bin/python scripts/prepare_webui_demo.py
bash scripts/start_webui.sh
```

本次安装并验证了 Streamlit 1.66.1、pandas 3.1.0。无需在本机安装 PySpark，也无需重建 Docker 镜像。prepare_webui_demo.py 校验前几阶段的本地 JSON，生成 outputs/webui/demo_manifest.json，不启动 Spark。本次注册 13 份快照。

访问地址：

- Ubuntu 本机：http://127.0.0.1:8501
- 本次 Windows 访问：http://192.168.142.137:8501

启动脚本绑定所有网卡；若虚拟机 IP 变化，用 hostname -I 查询实际网卡地址，使用 Ubuntu 的地址而非 Docker 的 172.x 地址。Windows 与虚拟机需要网络互通。本次用户已从 Windows 浏览器确认“能正常访问”，没有更改防火墙、虚拟机网络或集群配置。

## 8501 访问限制方案（仅报告，未应用）

2026-10-28 加固时只读检查：`scripts/start_webui.sh` 显式配置 `0.0.0.0:8501`，实际监听一致；Ubuntu NAT 网卡为 ens33，地址 192.168.142.137/24，网关 192.168.142.2，本机健康检查正常。监听所有网卡会使所有网络可达来源都有机会访问页面，包括触发未缓存的 Spark 请求；NAT 本身不等于用户认证。本次没有停止 / 重启 WebUI，未更改启动脚本、Streamlit 配置、防火墙、VMware 或集群配置。

建议保留现有监听和 Windows URL，通过 **Ubuntu 虚拟机入站过滤，只限制 TCP 8501**：先允许本机 loopback，再允许 ens33 上 Windows VMnet8 的实际源 IPv4 到本机 8501，最后拒绝其他来源到 8501，检查规则顺序使专用允许项在拒绝项之前。不要放行整个 /24 网段；同时检查 IPv6，防止以后监听 IPv6 时绕过限制。UFW 支持按来源 IP 和端口设置允许项，见 [Ubuntu 防火墙文档](https://ubuntu.com/server/docs/how-to/security/firewalls/)。这是方案，不是已应用的规则。

实施前需要确认的输入与顺序：

1. Windows 执行只读 `Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias '*VMnet8*'` 查宿主机虚拟网卡地址，在 Windows 打开页面时用 Ubuntu `ss -tn '( sport = :8501 )'` 核对实际来源。当前没有活动连接可确认源地址；不能把 VM 网关 .2、Windows 物理网卡或猜测的 .1 当作已确认来源。
2. 在 Ubuntu 控制台核对现有防火墙状态和规则，记录现状及本次规则的回退方式。若已有 UFW 则只准备 8501 的有序规则；若尚未启用，不直接执行 ufw enable 或改全局默认策略，应先单独审阅仅作用于本机 INPUT/8501 的规则，避免影响 SSH 和现有 Docker 通信。不改 FORWARD、DOCKER、DOCKER-USER、NAT 或集群端口。
3. 将准确源地址代入规则后再由操作者授权应用；源地址若经额外转发被共享，应先确认拓扑，避免把共享网关加入白名单造成过宽放行。本方案不需要为 Windows 宿主机直连 VM NAT 地址新增端口转发，也不切换桥接网卡。
4. 应用后分别新建连接验证：Ubuntu 的 `127.0.0.1:8501/_stcore/health` 和 Windows 的原地址页面 / WebSocket / 下载可用，未授权来源连接被拒绝。防火墙可能保留已建立连接，不能仅凭旧浏览器会话判断规则有效；同时只读核对集群健康。失败时只撤回本次 8501 规则，不清空防火墙或重建容器。

将监听直接改为 127.0.0.1 会中断 Windows 按原 URL 直连，因此不作为本轮方案。保持 CORS / XSRF 保护开启；本机独立检查进程解析到两项均为 true，启动脚本没有关闭它们，但未声称读取了在线进程的全部有效配置。这些选项不能替代源地址限制或登录认证，见 [Streamlit 配置说明](https://docs.streamlit.io/develop/api-reference/configuration/config.toml)。方案应用前，当前监听风险仍然存在；本轮不新增账号、代理或 TLS 业务功能。

## 页面功能与演示流程

1. 数据概览：用户数来自 user_activity.csv 汇总，评分数来自 rating_distribution.csv 汇总，图书数来自本地 books.csv 唯一 book_id。展示已有评分分布、热门图书、用户活跃度 CSV 及三张原 Matplotlib PNG。查看页面不扫描 ratings.csv，也不启动 Spark。
2. 用户阅读偏好：输入 660、1000、2000，可直接读取前阶段 JSON。展示评分数量、均分、完整 1～5 分分布、主题 Top 10 与贡献书、原始作者署名组合、代表性高分图书及 JSON 下载。页面说明依据为评分历史，不表示实际读完。
3. 推荐：选择 ALS 或 Popularity，展示 Top 10。ALS 标为“预测评分”，Popularity 标为“训练集评分次数”；分别显示请求和实际算法。999999 的 ALS 请求明确提示热门回退。模型原始预测不做 1～5 截断，下载 JSON 保留原始值。

推荐演示快照覆盖 660、1000、2000、661、999999 的两种模式。偏好快照覆盖 660、1000、2000。其他请求默认提示本地无有效结果；用户需勾选“没有缓存时使用 Spark 计算”并提交表单才会计算。修改表单、切换页面和下载结果不自动重算。

空用户、无高分、无主题及空推荐会显示已有模块的明确提示。无效输入、缺失展示资源、作业失败、超时和 Docker 不可调用会显示友好信息；失败请求会清除当前区域旧结果，避免继续展示另一个用户的旧数据。

## 缓存范围与失效

- 概览使用 st.cache_data，键包含各 CSV、PNG 的修改时间和文件大小。展示资源变化后自动重载。
- st.cache_resource 在一个 Streamlit 进程的不同会话间复用 WebBackend。页面会话保存已提交结果，普通交互不重复调用后端。
- 演示快照是指定数据版本的已验证结果，不设时间过期；每次读取检查源 JSON 摘要、代码指纹及数据版本。页面明确标为“预计算演示结果”，显示原文件生成时间。
- 按需缓存键包含请求类型、user_id、mode、Top-K、代码指纹和数据版本，保存到 outputs/webui/cache。默认有效期 3600 秒，跨浏览器和应用重启复用。页面区分“本次 Spark 计算”与“按需计算缓存”。
- 指纹覆盖 preferences.py、recommenders.py、prepare_tags.py、train_final.py、web_job.py 和 web_backend.py；这些实现变更后旧缓存和演示清单失效。原阶段 JSON 改动也会使其演示条目失效。
- 无法自动检测 HDFS 中外部替换的数据或模型。更换后需设置新的 BOOKREC_DATA_VERSION 并在计划窗口重启页面；旧演示与按需缓存均不能用于新版本，更新步骤见下文。内置 v1 演示注册入口只接受原版本，阻止将旧快照重新标为新数据 / 模型。
- 缓存到期、损坏或版本不匹配时，只有用户明确允许按需计算才会提交作业。同进程最多执行一个按需作业；忙时提示稍后重试，缓存结果仍可读取。

结果、日志与缓存都位于现有 .gitignore 忽略的 outputs/webui 目录，不改动前几阶段产物。

## 数据 / 模型版本更新流程

现有 `BOOKREC_DATA_VERSION` 已足以承担当前项目的缓存命名空间，无需每次刷新扫描 HDFS 或加载 Spark 模型。它在 WebBackend 创建时从环境读取一次；未设置时仍使用 `goodbooks10k-processed-v1-als_full_v1-topics_v1`，空字符串或纯空白直接报错。标识原样参与代码指纹，同一版本的多个后端进程可复用未过期的磁盘缓存；数据 / 模型版本变化则隔离缓存，不删除旧文件。TTL、代码指纹输入、JSON 摘要与快照校验机制保持不变。

版本是操作者声明的已验证产物身份，**不是数据集 / 模型路径选择器**。未来发布时，标识至少对应数据集及批次、内部 book_id 映射、processed / 标签版本和 ALS 模型产物批次，例如 `catalogA-r2-ids1-processed2-topics1-als003`。同名路径中的内容变化、仅更换模型、仅改变 ID 映射或标签策略时也必须换标识；不同内容不能复用旧标识。可以引用发布时离线记录的校验和，但不在页面请求中计算全量摘要。

未来更换资源时的人工流程（本轮未执行）：

1. 在页面外确认新的数据、ID 映射、标签和模型相互匹配，记录其路径、产物版本及新的唯一标识。先准备好实际资源；单独改环境变量不会切换或更新 HDFS 内容。
2. 保留旧数据 / 模型、原 v1 快照及清单备份；更新实际读取路径或资源后，在计划窗口用新标识启动页面，例如 `BOOKREC_DATA_VERSION=catalogA-r2-ids1-processed2-topics1-als003 bash scripts/start_webui.sh`。当前运行中的后端不会自动读取修改后的 shell 环境；需要重新创建后端 / 重启页面。本轮不重启在线页面。
3. 新版本首次查询默认提示没有缓存，只有明确允许计算才启动 Spark。检查请求所用数据 / 模型版本及结果后再开放使用；不能靠延长 TTL 或重新标记旧 JSON 来迁移结果。
4. 当前 `prepare_webui_demo.py` / `prepare_demo_manifest()` 只注册固定 v1 目录，非原版标识一律拒绝且不改写已有清单。未来新数据集若需要预计算演示，应另行设计带来源版本的新快照注册；本轮不扩展这一业务入口。原 v1 文件不能被放进新版本命名空间。

同一数据 / 模型下仅有校验或文档维护时，不为此更换数据版本。本轮标签正常计算及推荐逻辑未变，已备份运行时清单，只刷新代码指纹，保留原数据版本及 13 个条目的路径、摘要、生成时间。历史 artifacts 和结果内容不改写；未来修改实际计算语义时应先重新验证结果，不能照搬此刷新操作。

## 子进程与文件边界

user_id 只接受 32 位范围内的 ASCII 正整数；Top-K 限制为 1～50，页面固定为 10；mode 使用固定枚举。命令为参数列表，显式 shell=False，不拼接 shell 命令。

每个请求由本机创建 UUID 目录 outputs/webui/jobs/<id>，容器只写该目录内的新 result.json。桥接 CLI 限制输出位置，先写临时文件再原子重命名；本机校验用户、算法、字段、排序、排名和分数有效性后写缓存。Spark stdout/stderr 直接进入该请求的 spark.log，页面不解析或展示原始日志。

容器 GNU timeout 在 180 秒发送 TERM，15 秒后可强制终止作业进程组；本机 subprocess.run 的总超时为 210 秒。这样不只终止 docker 客户端的等待。目录和日志保留用于排查；本阶段没有自动删除历史产物。

## 自动验证

```bash
# 适配层（含版本隔离）和 Streamlit AppTest，均不提交 Spark
.venv/bin/python -m unittest discover -s tests -p 'test_web*.py' -v

# 读取演示快照，并通过 Docker 实际计算 662 和未知用户的结果
.venv/bin/python scripts/validate_webui.py
```

真实桥接验收将报告写入 outputs/webui/validation.json，目标已存在则拒绝覆盖。重跑可追加 `--report outputs/webui/validation-2.json` 指定新的报告文件。已有请求通常命中按需缓存，因此不保证再次提交新作业。其测试不更改模型，也不重算正式评价。

2026-10-28 版本管理完善轮次：16 项后端测试与 6 项 AppTest 全部通过，未执行上述真实桥接验收。测试验证环境变量默认值与一次读取、空值拒绝、数据集 / 数据批次 / 仅模型版本变更时缓存隔离、同版本跨实例复用、旧命名空间保留及 v1 快照防误标记；原 TTL、代码指纹、快照摘要失效测试继续通过。单独读取现有 13 份演示均命中预计算结果，无需 Spark。

可选真实浏览器测试：

```bash
.venv/bin/python -m pip install -r requirements-web-test.txt
.venv/bin/python -m playwright install chromium
.venv/bin/python scripts/test_webui_browser.py --url http://127.0.0.1:8501
```

Chromium 测试需要主机相应图形共享库，与 WebUI 运行依赖无关。本次主机缺少这些库，测试工具和所缺 Ubuntu 库只下载、解压到 /tmp，未安装系统包。本次测试使用 PLAYWRIGHT_BROWSERS_PATH=/tmp/bookrec-playwright 和 LD_LIBRARY_PATH=/tmp/bookrec-browser-libs/extracted/usr/lib/x86_64-linux-gnu。Windows 用户访问页面不需要 Playwright 或这些库。

浏览器测试检查真实图片加载、页面导航、JSON 下载、两种模式及回退，截图保存在 outputs/webui/browser。AppTest 的官方用法见 https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest 。

## 本次实测（2026-10-27）

- Ubuntu 本机与 192.168.142.137 局域网 HTTP 页面及健康检查均返回 200；Windows 用户确认可正常访问。
- 14 项后端与 Streamlit AppTest 全部通过，本机 pip check 通过。真实 Chromium 经局域网地址验证三张图片、导航、JSON 下载、两种模式和回退，无 JavaScript 页面异常。
- 数据概览为 53,424 用户、10,000 图书、5,976,479 条评分。
- 演示画像分别为：660 有 132 本评分、均分 4.0530；1000 有 107 本、均分 3.4112；2000 有 148 本、均分 3.9324。
- 三个用户的 ALS / Popularity 各返回 10 条；999999 请求 ALS 时实际为 Popularity，有明确回退提示。661 的 5.013201 预测在页面中保留，未截断到 5。
- 本机读取 3 份画像和 8 份推荐演示结果，启动 Spark 次数为 0。
- 用户 662 的未缓存画像、ALS、Popularity 首次分别耗时 26.297、27.803、26.034 秒，各启动一次 Spark。999999 的未缓存空画像耗时 20.884 秒，返回 user_not_found、平均分 null。
- 上述四个请求再次调用均命中按需缓存，新增 Spark 作业数为 0；662 的两模式推荐均未包含其原始评分历史中的图书，画像数量也独立核对一致。本机未导入 PySpark。
- 使用全新的 Ubuntu Python 进程重复验收，四个请求仍全部命中磁盘缓存，新增 Spark 作业数为 0，报告为 outputs/webui/validation-cached.json。

## 性能与后续建议

本版适合单机课程演示。未缓存请求包含 Spark 应用启动和文件读取，通常需要数十秒，页面采用等待提示和有界超时，不承诺即时结果。缓存不能替代模型更新或数据版本管理，演示快照也不是实时数据。

未来如需要更多并发用户，可在保持算法接口的前提下引入长驻计算服务和任务队列，减少每次 Spark 启动成本；本阶段未新增服务、数据库或调整集群配置。部署范围目前为现有课程网络，未实现用户账号和访问权限系统。
