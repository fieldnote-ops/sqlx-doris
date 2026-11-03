<a name="doc-top"></a>

# 测试说明

[English](TESTING.md) | [简体中文](TESTING.zh-CN.md)

[文档首页](README.zh-CN.md)

所有源码工具要求 Python 3.12+，统一通过 scripts/dev 运行。

<a name="doc-contents"></a>

## 目录

- [外置存储](#doc-storage)
- [无数据库检查](#doc-checks)
- [真实 Doris 测试](#doc-live)
- [验证范围与边界](#doc-coverage)
- [Kafka 故障夹具](#doc-fault-fixtures)
- [Kafka 只读预检](#doc-kafka)
- [定向执行与随机化](#doc-targeted)
- [安装验收](#doc-installation)
- [2026-11-02 验证记录](#doc-record)

<a name="doc-storage"></a>

## 外置存储

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
./scripts/dev --print-env
./scripts/dev cargo fetch --locked
```

可用 SQLX_DORIS_BUILD_ROOT 选择外置盘内的项目目录。脚本验证挂载点及解析符号链接后的目标，拒绝落到盘外。CARGO_HOME、CARGO_TARGET_DIR、CARGO_BUILD_BUILD_DIR、TMPDIR、缓存、日志、下载和夹具都在盘内；不要直接运行 cargo 或指定盘内以外的 target-dir/build-dir。

本地数据库必须先检查 Docker/Lima 虚拟机磁盘的物理位置。Docker 命名卷位于 VM 内时，VM 磁盘必须在外置盘；bind mount 的源目录也必须在外置盘。检查 docker inspect 的 FE 元数据、BE storage、日志卷和 docker info 的数据根目录。检查通过后才启动、拉取或创建夹具。不要把 Doris 服务源码、镜像、数据卷或 VM 磁盘放进本仓库。

<a name="doc-checks"></a>

## 无数据库检查

scripts/check 执行格式、Clippy、单元/协议/HTTP 模型测试、存储约束检查、无默认特性构建、API 文档检查、cargo package 和独立消费者依赖验证。普通 cargo test 中标记 ignored 的真实数据库测试代表未执行。

```sh
./scripts/dev ./scripts/check
./scripts/dev ./scripts/test-msrv
./scripts/dev ./scripts/test-docs
```

文档门禁检查 21 对中英文页面、稳定锚点、同语言导航、相同代码/命令及表格数值。test-docs 编译去重后的普通 Rust 示例，并只检查 shell 语法，不执行其中的命令。要额外编译查询宏示例，需提供真实生成的外置元数据：`./scripts/dev ./scripts/test-docs --query-metadata "$SQLX_OFFLINE_DIR"`。默认检查未执行的元数据依赖示例会在汇总中明确列出。

<a name="doc-live"></a>

## 真实 Doris 测试

使用专用 Doris 4.1.5 数据库 sqlx_doris_test；账号需要 CREATE、ALTER、DROP、查询及物化视图等测试权限。测试会创建随机名称对象与 sdk_migrate_/sdk_cli_ 前缀临时数据库，成功后清理；失败中断可能留下这些测试对象。迁移与 CLI 用例需要建删临时数据库权限。

```sh
export DORIS_TEST_URL='doris://root@doris-fe:9030/sqlx_doris_test'
export DORIS_HTTP_URL='http://doris-fe:8030'
export DORIS_BE_HTTP_URL='http://doris-be:8040'
export DORIS_LOAD_FIXTURE_HOST='<host reachable from FE and BE>'
export DORIS_KAFKA_BROKERS='<dedicated Kafka broker:9092>'
export DORIS_KAFKA_TOPIC='<dedicated existing topic with partition 0>'
export DOCKER_HOST='<previously audited external Docker daemon socket>'
export DORIS_KAFKA_CONTAINER='<dedicated official Kafka container>'
export DORIS_KAFKA_CONNECTION_FILE="$SQLX_DORIS_EXTERNAL_VOLUME/kafka/ssl.json"
export DORIS_KAFKA_SASL_CONNECTION_FILE="$SQLX_DORIS_EXTERNAL_VOLUME/kafka/sasl.json"
export DORIS_KAFKA_FAULT_MANIFEST="$SQLX_DORIS_EXTERNAL_VOLUME/kafka-faults.json"
./scripts/dev ./scripts/prepare-test-certificates
./scripts/dev ./scripts/prepare-load-fixtures
./scripts/dev ./scripts/test-live
./scripts/dev ./scripts/test-consumer --live
```

HTTP 账号可用 DORIS_HTTP_USER、DORIS_HTTP_PASSWORD 设置。SQL TLS 用例要求专用 FE 实际开启 `enable_ssl=true`，使用 prepare-test-certificates 生成的两份 PKCS12（测试密码均为 fixture）：

```properties
mysql_ssl_default_ca_certificate = <external fixture path inside FE>/ca_certificate.p12
mysql_ssl_default_ca_certificate_password = fixture
mysql_ssl_default_server_certificate = <external fixture path inside FE>/server_certificate.p12
mysql_ssl_default_server_certificate_password = fixture
```

将文件放到已审计外置 VM 的专用 FE 并按 [Doris SQL TLS 配置](https://doris.apache.org/docs/4.x/admin-manual/auth/certificate/) 重启该测试 FE。证书仅含 DNS localhost，测试用它验证正确主机名，并以 IP 和错误 CA 验证拒绝。SQL TLS 不能用 HTTP 代理替代；HTTPS Stream Load 用例另外运行本地 Python 代理。证书与代理日志都在外置盘；夹具生成需要 openssl，Parquet/ORC 生成依赖装到外置盘 Python 环境。缺少环境变量、TLS 配置或夹具直接失败。

测试覆盖 SQLx 查询与 FromRow、原生类型与精度、复杂类型、字节绑定、事务提交/回滚/断连、查询取消与流中断、连接池恢复、Schema、物化视图、Stream Load 和 HTTPS。test-live 还会执行宏消费者与 CLI 端到端契约：在线/离线查询、六种查询宏、重命名依赖、FromRow 属性、参数/输出类型错误、编译不执行写入、prepare/--check、迁移互斥、checksum/dirty、异步 ALTER、取消恢复、目标版本及单步撤销。结果在 SQLX_DORIS_ARTIFACTS_DIR 下的 check.log、live-full.log、live-full-summary.json、consumer-live-full.log；脚本要求真实测试实际执行且全部通过。

性能测量见 [性能说明](PERFORMANCE.zh-CN.md)。不要把测试日志和阶段报告复制回仓库。

<a name="doc-coverage"></a>

## 验证范围与边界

| 范围 | 核验内容 |
| --- | --- |
| 驱动与绑定 | SQLx 查询、FromRow、精度、Unicode/NUL/原始字节、两种 SQL 模式、事务、取消、连接池与唯一预编译句柄关闭；畸形报文必须淘汰连接，不重放写入。 |
| 查询宏与离线缓存 | 六种宏、重命名依赖、调用者路径、外连接可空性、类型覆盖、负面编译及编译时不执行写入；缓存跨进程互斥、损坏修复、暂存失败与符号链接拒绝。 |
| 迁移与恢复 | checksum/dirty、异步 ALTER、目标版本/撤销、UUID 主锁及转换锁；断连、future/进程取消、重复恢复、owner 变化和迟到 DROP 均保留后继与历史。取得主锁后遇到转换门而超时，连接必须退役并保留主锁供显式检查。 |
| 身份与排空 | 独立账号创建/退役、权限与 host 校验、私密凭证、重连拒绝、现存事务及 COLUMN/ROLLUP/LOAD/MV/Routine 作业观察；精确事务 abort、TaskId 取消和 UUID STOP 只在显式维护条件下提交一次。 |
| Routine 与真实消息 | receipt/确认文件、名称不复用、CREATE/STOP 的迟到投递及 ACK 丢失；实际 Kafka 消息、BE 已确认在途批次、两种提交/STOP 顺序、逐分区游标与 DUPLICATE KEY 行重数。特权绕过名称协议的负面测试必须展示其边界。 |
| CLI 与诊断 | 全命名空间的人类/统一 JSON 输出、退出码 0/2/3/4/5/6/7、秘密隐藏、阶段优先级；info/diagnose 不建表，独立失败保留部分观察、原因码与只读后续命令。 |
| 包与依赖 | 五个源码包、十一种隔离消费者及 ops 的空/helpers/stream-load/cli-support/maintenance/kafka 组合；默认和 migrate-only 不带 HTTP/CLI，基础 CLI 不带维护 HTTP/Kafka，所有 CLI 不带查询宏，实际构建图无其他数据库驱动。 |

故障测试在真实 FE/BE 前设置报文屏障，精确控制提交前、服务端执行后丢 ACK 和确认后的核验断连；允许已经提交的请求在取消后完成。每个边界检查提交次数、连接淘汰、锁与 dirty 保留。客户端 owner 检查不是业务 SQL 的原子 fencing；退役账号不能撤销已提交语句，也不能阻止已准入事务 COMMIT 或异步作业继续完成。

LOAD 的 S3 文件响应夹具只模拟外部文件服务，事务和作业均为真实 Doris。Kafka 消息由专用官方容器的管理工具产生，acks=all 后核对真实 offset；产品 CLI 仍只读 Kafka。FE PREPARE 不等于 BE 已准入，测试还要求匹配 JobId/TxnId、正 BeId 与有效执行开始时间。BE 资源拒收、未知状态、缺消息或意外重复直接失败，不自动调配额。STOP 后必须再排空并核对行重数；测试不认证生产 exactly-once、任意元数据恢复、多 FE 或完整服务端排空。

<a name="doc-fault-fixtures"></a>

## Kafka 故障夹具

矩阵使用当前官方 Doris 4.1.5、官方 Kafka 4.0.0 的公共 CLI，不给产品引入 Kafka 客户端依赖。按 keyed producer 的 Kafka 分区规则选择测试 key，并通过 broker 每分区真实 high watermark 精确增量独立验证消息落点；同时记录 cluster/topic ID、earliest/end、leader/ISR 和 SQL 的 ID 重数。DUPLICATE KEY 防止覆盖掩盖重复，fixture 数据不代表端到端 exactly-once。

运行完整测试前，需在同一专用 Kafka 集群准备两个**独占的 broker-only 故障节点**，与共享 controller 分开。所有 VM 磁盘、broker 数据/配置/日志卷均须先确认在外置盘；不自动拉镜像。为每轮生成一个随机 UUID owner，两节点 Docker label `sqlx-doris-fixture-owner` 与该 owner 相同，名称以 sqlx-kafka-matrix- 开头；KAFKA_NODE_ID 为两个不同且大于 1 的 ID，CLUSTER_ID 与专用集群一致。数据必须使用 sqlx-kafka-matrix- 前缀命名卷（挂载到 /var/lib/kafka/data），其他镜像卷（含自动创建的匿名卷）也必须是已审计外置 VM 内的本地 Docker 卷。节点使用相同的已缓存官方 Kafka 镜像。

基线专用 node 1 须同时提供 broker 与 controller；两个故障节点使用同一静态 controller quorum，且不能承担 controller。容器在同一专用网络，公布各自独立且 Doris/其他 broker 可达的 hostname。官方 Kafka 4.0.0 镜像的关键环境如下；节点 ID/hostname 必须分别替换，数据目录位于该节点的独占命名卷，镜像 ID 与 manifest 一致：

```properties
CLUSTER_ID=<existing cluster ID>
KAFKA_NODE_ID=<this broker ID greater than 1>
KAFKA_PROCESS_ROLES=broker
KAFKA_LISTENERS=PLAINTEXT://:9092
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://<this broker hostname>:9092
KAFKA_CONTROLLER_QUORUM_VOTERS=1@<existing controller hostname>:9093
KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
KAFKA_LOG_DIRS=/var/lib/kafka/data/kraft-combined-logs
```

DORIS_KAFKA_FAULT_MANIFEST 是 0600 外置普通文件，不接受文件符号链接。示例结构（替换为实际运行节点与集群，不要填共享或业务 broker）：

```json
{
  "owner": "<32 hex UUID>",
  "cluster_id": "<actual cluster ID>",
  "image": "sha256:<actual official image ID>",
  "nodes": [
    {"id": "<exact Docker ID>", "node_id": 2, "name": "sqlx-kafka-matrix-<owner>-2"},
    {"id": "<exact Docker ID>", "node_id": 3, "name": "sqlx-kafka-matrix-<owner>-3"}
  ]
}
```

尾部回退测试会显式停止/启动这两个节点；每次先核对 Docker ID、名称、nonce label、node/cluster ID、镜像和卷。共享 controller 不被停止，不改变全局配额或放松 Doris 锁/账号隔离。只在 sdk_rl_matrix_UUID 主题上配置副本和删除记录。成功后恢复两个 broker 并删除自己的主题；失败保留现场供检查，不重放未确认的业务操作。测试必须串行，不能与其他用户或生产流量共享故障节点。

```sh
./scripts/dev cargo test --locked --offline -p sqlx-doris --all-features --test migrate_live kafka_matrix_ -- --ignored --nocapture --test-threads=1
```

场景包含：两分区独立提交与 STOP 的混合结果、最终检查后提交先赢、错误全局最大值造成缺失、旧快照重放、非零初始游标并非消费证明、显式分区列表不自动覆盖新增分区、动态发现的 BEGINNING 与默认 END、真实 DeleteRecords 前缀过期、同名 topic 换代，以及同 topic ID 下通过停止副本/公共 unclean election 造成真实 end 回退并重新占用 offset。动态发现用例只暂停/恢复同一尚未退役的活跃创建者作业以固定发现时序，不把 STOPPED UUID 交给后继。

证据 JSON 和日志写入外置 SQLX_DORIS_ARTIFACTS_DIR，包含真实事务终态、分区游标、broker 范围和精确 SQL 行重数；每个接续作业 STOP 并排空后再次核对重数，不能只以运行中的短暂匹配作为通过；资源拒收、缺失消息、意外重复或未知状态直接失败，不计通过。

<a name="doc-kafka"></a>

## Kafka 只读预检

SSL 和 SASL_SSL/PLAIN 配置均为外置 0600 普通文件，格式见[使用说明](USAGE.zh-CN.md)。客户端须直连所有 advertised listener，证书覆盖公布的主机名；不同 listener 必须属于同一 cluster/topic，不能改写 Kafka 元数据地址。测试包含多 leader、分区增加、DeleteRecords 前缀过期、epoch 锚点丢失、topic 换代及同 topic ID 下的真实尾部回退，同时核对 TLS/SASL 失败和源状态不变。新建事后基线不能证明此前历史。独立 Java 工具使用 128 MiB 堆；仍须为工具、broker 和 SSL 开销预留内存，OOM 直接失败。

<a name="doc-targeted"></a>

## 定向执行与随机化

driver 组只需 DORIS_TEST_URL，使用空驱动特性；ingestion 组需要 SQL/HTTP 端点、TLS 与文件夹具，只开启 stream-load。定向检查不能代替默认 full 发布门禁，后者仍要求 Kafka/故障夹具，并执行宏、CLI 与示例。汇总保留每个实际执行的测试名。

```sh
./scripts/dev ./scripts/test-live --group driver
./scripts/dev ./scripts/test-live --group ingestion
./scripts/dev ./scripts/test-macros
./scripts/dev ./scripts/test-cli
./scripts/dev ./scripts/test-msrv --macro-consumer
./scripts/dev cargo test --locked --offline -p sqlx-doris --all-features --test migrate_live transition_ -- --ignored --test-threads=1
./scripts/dev cargo test --locked --offline -p sqlx-doris --all-features --test migrate_live main_partition -- --ignored --test-threads=1
./scripts/dev cargo test --locked --offline -p sqlx-doris --all-features --test migrate_live identity::drain -- --ignored --test-threads=1
./scripts/dev cargo test --locked --offline -p sqlx-doris --all-features --test kafka_preflight_live -- --ignored --test-threads=1
DORIS_RANDOM_SEED=9417 DORIS_RANDOM_CASES=16384 ./scripts/dev cargo test --locked --offline -p sqlx-doris-core --all-features randomized -- --nocapture
```

四组随机化合约直接调用协议长度、行解析和绑定实现，覆盖 NULL、数值、原始字节、截断、畸形包及 Unicode SQL。默认每组 4096 例，可配置 64–100000 例；失败种子可复现，corpus 和日志均外置。MSRV 宏消费者须先由 test-macros 生成外置夹具。全量原生测试必须串行；普通测试中的 ignored 不计通过。

<a name="doc-installation"></a>

## 安装验收

```sh
./scripts/dev ./scripts/test-install
# Only reuse five freshly verified, byte-matching archives:
./scripts/dev ./scripts/test-install --reuse-packages
```

分别实装 basic/admin/admin-kafka 三种 CLI，再用全新应用验证通用 SQLx、query!、migrate!、已有表 Stream Load、项目 Cargo 配置、prepare/--check、强制离线重建以及不含 Kafka 的 Doris 诊断。安装验收需要上文 SQL 与可信 FE/BE HTTP 端点。五个内部依赖通过外置临时 Cargo 配置路由到原样解包的源码包，不修改源码包或上游 SQLx；此验收不代表公开注册表发布/下载。失败保留隔离库和现场，不自动恢复或重放；成功仅删除自己的临时库。

check.log、live-full.log、live-full-summary.json、native-macro-summary.json、native-cli-summary.json、msrv.log 和安装日志均在外置 SQLX_DORIS_ARTIFACTS_DIR。每份汇总只统计实际执行的契约。当前基线只验证官方 4.1.5，后续版本须重新验证元数据与事务语义，不投入低版本适配。持续负载与性能范围见[性能说明](PERFORMANCE.zh-CN.md)。

<a name="doc-record"></a>

## 2026-11-02 验收记录

191 项 Rust 单元/协议/HTTP/文档测试与 20 项 Python 文档/存储检查通过。Doris 4.1.5 / Kafka 4.0.0 修复后验收将完整运行记录与针对性复测逐项核对当前清单，共 144 项原生测试全部通过，无遗留失败或忽略；普通 cargo test 中的 144 项 ignored 不计入通过。最小驱动组额外通过 6 项，最小导入组通过 13 项，均不重复加入完整组计数。

| 功能域 | 实际执行 |
| --- | --- |
| 驱动 / 类型 / TLS | 15 |
| Kafka 预检 | 6 |
| Schema / MV / 应用助手 | 13 |
| 迁移 / 锁 | 41 |
| 身份 / 排空 / Routine / 数据竞态 / CLI | 56 |
| Stream Load / HTTPS | 13 |

3 个运行示例、23 项宏合同与 82 项 CLI 合同通过；11 个不同 Rust 文档示例按最小特性分别编译，其中查询宏使用真实元数据。21 对英中页面保持结构、命令与代码一致。

全目标 Clippy、严格 rustdoc、Rust 1.94 工作区测试及离线重命名消费者、11 个隔离消费者、6 组独立 ops 特性及交叉合并通过。四项随机合同分别使用种子 1/17/9417，每组 16384 个案例。5 个新鲜同版本源码包与工作区逐字节一致；cli、cli-admin、cli-admin-kafka 分别实装，完成全新应用的在线查询/宏/迁移/已有表导入、prepare/--check 和强制离线重建。公开注册表发布/下载未测试。

原始失败与验收样本保留在外置目录。存储错误和资源拒绝均判为失败；自有夹具重启保留数据，并核对相同的有效配置、配额和挂载。针对性修复与未执行组逐项核对当前完整原生清单，不将 ignored 计为通过。
