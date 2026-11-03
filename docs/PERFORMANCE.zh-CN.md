<a name="doc-top"></a>

# 性能验证

[English](PERFORMANCE.md) | [简体中文](PERFORMANCE.zh-CN.md)

[文档首页](README.zh-CN.md)

使用专用 Doris 数据库运行，账号需具备建删测试表与临时数据库的权限。所有工作负载使用 release 构建，先结束其他测试/编译任务，再串行测量。结果、缓存、外部消费者与测试数据均在外置盘。

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
export DORIS_TEST_URL='doris://root@localhost:9030/sqlx_doris_test'
export DORIS_HTTP_URL='http://localhost:8030'
export DORIS_BE_HTTP_URL='http://localhost:8040'
export DORIS_BENCH_ITERATIONS=500
./scripts/dev ./scripts/benchmark
```

benchmark 依次执行点查、宏、迁移和应用负载；各子脚本也可独立运行。应用负载要求显式配置的回环 SQL 端点（例如自有 SSH 隧道）及可信 HTTP 端点，保留 TLS 校验。

- 点查：单副本 Unique Key 表，开启行存储，按 BIGINT 主键读取一条 9 字节字符串。缓存预编译、每次 PREPARE/CLOSE、默认池、关闭获取前 ping 的池及文本查询各五轮，每轮 500 次，交替顺序并校验每次返回值。另测八并发池和十万行流式扫描。
- 宏：在同一连接、相同 SQL 与参数上交替比较 query! 和 query_as，读取 CAST 参数生成的 BIGINT 与字符串；预热后各五轮，每轮 500 次，校验每次返回值。此表衡量两种 Rust 查询 API；点查表衡量缓存及池配置。
- 迁移：单 BE 临时数据库、单副本元数据、默认持久锁。首次创建状态表及两个持久锁父表，并应用一份 SELECT 1 迁移；再应用五个增量版本，每次紧接一次无变更检查。此样例衡量协调、checksum、dirty、状态写入及迁移阶段 owner 检查点成本；实际 DDL 时间由数据规模和服务器作业决定。

外置 SQLX_DORIS_ARTIFACTS_DIR 保存 benchmark.json、benchmark-macros.json、benchmark-migrations.json 和对应日志，保留每轮原始值。除首次迁移及附加并发/扫描样本外，测量汇总均为各轮指标的中位数，分位数也按轮取中位数。

<a name="doc-contents"></a>

## 目录

- [调优](#doc-tuning)
- [应用查询与导入](#doc-application)
- [CLI 测量](#doc-cli)
- [持续混合读取](#doc-sustained)
- [结果解释](#doc-interpretation)
- [2026-11-02 产品验收实测](#doc-record)

<a name="doc-tuning"></a>

## 调优

重复 SQL 保持相同查询形状并保留默认 statement cache；默认每连接缓存 100 条。按服务器容量配置 max_connections，使用 min_connections 预建连接。test_before_acquire(false) 可避免获取前的 ping；这时空闲失效连接会在首次业务命令暴露错误，驱动仍不重放写入。默认池沿用 SQLx 的健康检查。

大批量导入使用 Stream Load 或批量写入，大结果使用 fetch 流式处理。查询宏在编译时获取类型元数据，运行时使用同一 Doris 执行器及安全绑定。迁移锁、状态查询与异步 DDL 轮询只在迁移调用中发生，健康的普通查询不增加这些往返。

<a name="doc-application"></a>

## 应用查询与导入

```sh
./scripts/dev ./scripts/benchmark-workloads --rows 4096 --width 1024 --rounds 3 \
  --iterations 25 --seed 9417 --delays 0 2
```

固定种子的 Duplicate Key 数据集包含 BIGINT/INT、1024 字节 UTF-8 payload 与 ARRAY<BIGINT>，核对每个返回值和行重数。每组先预热两次，再执行三轮各 25 次查询。聚合与 JOIN 处理 4096 行输入、返回 16 组；宽列/复杂类型/慢消费者分别返回 256 或 4096 行，慢消费者每 64 行等待 1 ms。两个转发配置使用相同 SQL 与绑定值。

0 和 2 ms 配置使用同一个本地 TCP 转发器；双向每个最多 65536 字节的数据块分别延迟，不等于固定 RTT，也不复现生产网络。JSONL 保存结果行数/payload 大小、逐次延迟、转发字节/块数和服务端版本。

六种导入模式每轮各一次，并交替顺序：每批 256 行的 INSERT、send_json、send_bytes、send_file、sync Group Commit 和 async Group Commit。仅自有测试表设置 group_commit_interval_ms=1000，不修改全局配额。INSERT 每批分别提交，Stream Load 批次有自身事务，两者不具有相同的整体原子性。独立记录 serde_json 编码基线、文件准备/fsync、提交回执及行数可见耗时；随后完整核对所有值与重数。要求服务端确认实际 Group Commit 模式，每次导入拒绝过滤。payload MiB/s 使用编码后的 JSON 大小，不是实际网络字节。

每个配置设置墙钟预算（默认 --max-seconds 1800，范围 30–7200），RSS 采样也随之有界。超时判定负载失败，只停止自身子进程；已获准的服务端工作仍可能完成，因此保留现场且不重放。

外置 workloads-UUID/summary.json 保留原始样本、最近秩 P50/P95/P99、校验后的 rows/s 与 payload MiB/s；每种导入只有三个样本，尾分位仅作描述。RSS 每 50 ms 采样，包含期望数据及编码基线留下的分配器历史，不能隔离上传缓冲分配，也不是绝对峰值。文件请求有界分块读取、字节请求在重定向时共享存储，这些实现性质与 RSS 实测分开说明。失败保留本轮命名表和外置输入，不重试写入、不提高配额。

<a name="doc-cli"></a>

## CLI 测量

```sh
./scripts/dev ./scripts/benchmark-identities
./scripts/dev ./scripts/benchmark-routines
./scripts/dev ./scripts/benchmark-kafka --connection-file "$KAFKA_CONFIG" \
  --topic events --offset 0=123 --offset 1=456
./scripts/dev ./scripts/benchmark-diagnostics --retired-identity "$OLD_ID"
```

身份基准在隔离库测量五轮创建、只读退役预览、显式退役和空库 drain。Routine 基准还要求 DORIS_KAFKA_BROKERS / DORIS_KAFKA_TOPIC（已有 partition 0），使用 OFFSET_END，不生产消息；每轮单独身份准备不计入测量，记录 JSON 创建、按名称的人类状态、receipt JSON 状态/停止预览/显式停止。成功只清理自己的隔离库；永久 reservation 的数量随代际增加。receipt 路径包括物理盘、私密文件与代际检查，不能与纯名称状态当作同一工作量。

Kafka 基准要求稳定的独立主题，每个分区至少一条保留消息，显式候选覆盖完整分区集合并位于范围内。每轮捕获新基线，再做只读 preflight，包括 TLS/可选 SASL、分 leader 元数据和私密文件检查。脚本不产生消息、不提交/转移/重置 offset；测试消息须由独立夹具工具预先确认。新基线只用于本次测量，不证明此前历史。

诊断基准要求空闲、已核对的隔离库及真实已退役的独立身份；可添加 `--receipt "$RECEIPT"`。Kafka 观察必须一起显式提供 --connection-file/--topic/--checkpoint/--offset。五轮交替运行 lock/drain JSON 与 diagnose 人类/JSON 进程，全部须成功。它不创建身份、改变锁/历史/作业或推导 offset；空库结果不衡量真实排空时间。

CLI 指标是 release 进程墙钟时间，包含启动、网络、文件及元数据核验。五轮与小目录不足以证明固定输出开销比例、生产延迟或接续安全。原始向量保存在外置 benchmark-identities.json、benchmark-routines.json、benchmark-kafka-UUID.json 和 benchmark-diagnostics-UUID.json，不剔除单独慢样本。

<a name="doc-sustained"></a>

## 持续混合读取

```sh
./scripts/dev ./scripts/benchmark-sustained --seconds 60 --rounds 3 \
  --workers 16 --pool-size 4 --cache-capacity 8 --seed 9417
```

六类逻辑操作覆盖参数点查、同一连接连续 32 种 SQL 强制缓存淘汰、64 KiB 字段、延迟消费、读取首行后丢弃流，以及已验证首行后消费者停顿至客户端截止时间。截止时间不是服务端 SQL 超时。缓存上限为 1–16，必须小于 32 种形状，worker 必须多于池容量；校验每个已消费值。

默认三轮各 60 秒。固定内存直方图报告 1 ms 桶上界的排队/操作 P50/P95/P99，以及精确平均/最大值。操作包含不同语句数与人为延迟，operations/s 不是纯查询 QPS。每秒采样客户端 RSS，保留完整采样向量和最大观测值，不称绝对峰值或泄漏证明。取消后有意替换的累计新连接数也不能单独证明泄漏。

结束后同时取得所有池连接，验证 ping 与缓存上限，关闭池并等待隔离库会话从 FE 消失；不 KILL、不重试写入。客户端句柄数不证明服务端总句柄，协议测试另检查唯一 PREPARE/CLOSE 配对。失败保留数据库与日志，成功只删除自己的库；benchmark-sustained-UUID.json 和日志全部外置。

<a name="doc-interpretation"></a>

## 结果解释

仅比较完全相同 SQL、数据、服务器设置和负载下的实测结果。拆包消除了默认驱动的运维/CLI 依赖，普通查询仍走原生 Executor；这本身不证明 QPS 提升。运行时测量不包含编译。源码包安装使用已有依赖缓存，其 Cargo 时间不能视为冷构建或公开注册表下载时间。测试/安装范围见[测试说明](TESTING.zh-CN.md)。

<a name="doc-record"></a>

## 2026-11-02 产品验收实测

Apple M1 Max / 64 GiB / macOS arm64；外置盘 aarch64 Linux VM 为 4 vCPU / 12 GiB，镜像为 arm64，官方 Doris 4.1.5 单 FE/BE（FE heap 512 MiB、BE limit 3 GiB），Kafka 4.0.0。使用 rustc 1.99.1 release，经本机 TCP/TLS 与 SSH 转发。正确性检查结束后，专属 Doris 使用原配置/配额/数据重新启动，再串行测量；运行时指标不含编译。成功运行保留全部试次，失败证据单独保留；没有前后性能提升声明。

| 点查模式 | QPS | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- |
| 缓存连接 | 537.151 | 1.646 | 3.125 | 4.884 |
| 无缓存连接 | 179.378 | 5.356 | 7.508 | 18.154 |
| 默认连接池 | 483.037 | 1.983 | 2.929 | 4.922 |
| 不做获取 ping 的池 | 747.817 | 1.237 | 2.040 | 3.224 |
| 文本连接 | 271.615 | 3.610 | 4.655 | 7.255 |

| 同 SQL API | QPS | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- |
| sqlx_doris::query! | 291.411 | 3.384 | 4.439 | 5.772 |
| sqlx::query_as | 306.907 | 3.158 | 4.154 | 5.672 |

| 迁移 | ms |
| --- | --- |
| First apply (1) | 435.834 |
| Incremental apply (5 median) | 340.813 |
| Unchanged check (5 median) | 158.776 |

以上为五轮试次指标的中位数；首次迁移只有 1 次，实际 DDL 取决于数据和异步作业。

| 传输配置 | 查询 | 结果行 | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- | --- |
| query_relay_0ms | aggregate | 16 | 11.800 | 17.261 | 20.609 |
| query_relay_0ms | join | 16 | 15.506 | 24.680 | 32.405 |
| query_relay_0ms | wide | 256 | 11.891 | 16.727 | 24.739 |
| query_relay_0ms | wide | 4096 | 30.247 | 37.182 | 42.282 |
| query_relay_0ms | complex | 256 | 9.517 | 12.408 | 13.689 |
| query_relay_0ms | complex | 4096 | 10.317 | 12.812 | 14.622 |
| query_relay_0ms | slow_consumer | 256 | 20.380 | 23.166 | 26.966 |
| query_relay_0ms | slow_consumer | 4096 | 172.963 | 180.610 | 186.749 |
| query_relay_2ms | aggregate | 16 | 21.161 | 24.822 | 29.618 |
| query_relay_2ms | join | 16 | 23.943 | 28.792 | 33.083 |
| query_relay_2ms | wide | 256 | 31.495 | 34.902 | 39.496 |
| query_relay_2ms | wide | 4096 | 202.924 | 211.742 | 221.128 |
| query_relay_2ms | complex | 256 | 19.867 | 22.632 | 23.932 |
| query_relay_2ms | complex | 4096 | 23.145 | 25.966 | 26.428 |
| query_relay_2ms | slow_consumer | 256 | 33.573 | 38.716 | 51.795 |
| query_relay_2ms | slow_consumer | 4096 | 203.607 | 215.142 | 238.368 |

| 导入模式 | 样本 | ACK P50 ms | 可见 P50 ms | 可见 P95 ms | rows/s 中位数 | payload MiB/s 中位数 |
| --- | --- | --- | --- | --- | --- | --- |
| insert_batch | 3 | 984.663 | 998.085 | 1095.731 | 4103.860 | 4.272 |
| load_json | 3 | 117.162 | 135.584 | 136.226 | 30210.017 | 31.445 |
| load_bytes | 3 | 113.252 | 130.758 | 136.888 | 31325.073 | 32.606 |
| load_file | 3 | 113.154 | 129.178 | 142.305 | 31708.104 | 33.005 |
| group_sync | 3 | 1114.742 | 1132.270 | 1146.413 | 3617.513 | 3.765 |
| group_async | 3 | 52.634 | 1129.240 | 1137.394 | 3627.217 | 3.776 |

4096 行、1024 字节 payload、种子 9417，查询每组 3×25 样本、导入每种 3 样本。逐值和重数核对通过；完整原始编码/文件/ACK/可见时间、字节数与 RSS 向量均保留。导入三个样本的尾分位仅作描述，转发每块延迟不等于 RTT；RSS 包含校验数据与分配器历史。

| 轮次 | 秒 | 逻辑操作 | ops/s | 排队 P99 ms | 缓存最大 | 池 |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 60.305 | 2874 | 47.658 | 662 | 8 | 4 |
| 1 | 60.476 | 2937 | 48.564 | 627 | 8 | 4 |
| 2 | 60.686 | 2909 | 47.935 | 629 | 8 | 4 |

共 8720 次逻辑操作，182 个 RSS 样本，观测最大 12672 KiB；最终核对 4 个健康连接，关闭池后会话 0，自有库清理完成。三分钟、采样 RSS 与累计替换连接数均不证明长期无泄漏或生产吞吐。

| 专项 CLI（五轮中位数） | ms |
| --- | --- |
| Identity create_ms | 218.638 |
| Identity preview_ms | 62.785 |
| Identity retire_ms | 101.924 |
| Identity drain_ms | 127.432 |
| Routine create_ms | 302.241 |
| Routine status_ms | 225.952 |
| Routine status_name_ms | 79.993 |
| Routine preview_ms | 240.886 |
| Routine stop_ms | 314.072 |
| Kafka TLS snapshot_ms | 233.221 |
| Kafka TLS preflight_ms | 219.440 |
| Diagnosis lock_json | 69.317 |
| Diagnosis drain_json | 122.453 |
| Diagnosis diagnose_human | 365.936 |
| Diagnosis diagnose_json | 363.157 |

专项指标包含进程、网络与文件/元数据核验；Kafka 使用新鲜基准和显式候选，诊断包含退役身份与独立 Kafka 观察，不包含 receipt/source。空排空快照不代表实际排空耗时，也不证明接续或接管安全。资源拒绝与外置存储故障的失败记录保留；未提高配额或删除慢样本。
