<a name="doc-top"></a>

# Kafka 接续预检

[English](KAFKA.md) | [简体中文](KAFKA.zh-CN.md)

[文档首页](README.zh-CN.md)

高级选用功能：安装 `sqlx-doris` 的 `cli-admin-kafka` 特性；命令位于 `cargo sqlx-doris admin` 下。日常查询和 Stream Load 不要求此功能。

为管理员明确指定的接续候选值提供只读证据，不转移或重置 offset，也不授权创建后继作业或恢复锁。

<a name="doc-contents"></a>

## 目录

- [Progress、提交与分区变化](#doc-progress)
- [记录与核对源历史](#doc-preflight)

<a name="doc-progress"></a>

## Progress、提交与分区变化

真实 Kafka 消息、DUPLICATE KEY 表和服务端事务验证了两个顺序：确认 BE 已接收同一 JobId/TxnId 的 PREPARE 任务后，STOP 先完成，批次中止且不写入；最后一次 CLI 检查后，提交仍可能先于排队 STOP 完成，数据保留且 Progress 推进。FE 在向 BE 提交前就登记 PREPARE，BE 拒收也可能保留该事务；RUNNING 与 FE-only PREPARE 均不能证明数据正在处理。确认在途任务须核对同一 JobId/TxnId、正 BeId 和有效执行开始时间。账号退役不撤销已下发 BE 任务的内部提交授权；不能用“退役 + STOP 返回”推断无新提交。生产恢复仍需排空、核对事务终态、数据及所有分区。

4.1.5 的 JSON `job.progress` 是 Doris 报告的游标：数值通常为内部下一起点减一；它也可能只是**配置起点减一，尚无任何提交**。例如新作业从 partition 0 的 offset 3 开始，尚未写入任何行，Progress 就可能显示 `{"0":"2"}`。JSON 明确给出 `progress_semantics:doris_reported_cursor`、`progress_scope:job_reported_partitions_only`、`progress_proves_consumption:false` 和 `kafka_continuation_verified:false`。确认文件同样只确认创建绑定，不能充当已消费证明。[4.1.5 游标实现](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/load/routineload/KafkaProgress.java)。

多分区接续必须在旧作业终态、相关事务全部结束并核对实际数据后，逐 partition ID 解释最终游标，再检查 Kafka 当前代际、分区集合及可读范围。数字加一只是候选下一起点。例如最终游标 `{"0":"0","1":"2"}` 对应候选 `{0:1,1:3}`，不能取全局最大值 3 作为所有分区的起点。`kafka_partitions` 与 `kafka_offsets` 必须按同一顺序成对列出。STOP 前的快照可能在最后一次检查后继续推进，直接复制会重放已提交数据；必须使用 STOP 后重新核对的状态和事务/行数据。

| Kafka 变化 | 接续处理 |
| --- | --- |
| 新增分区 | 显式 kafka_partitions 列表不自动扩大；旧 Progress 缺少新分区不代表它无消息。新分区必须显式选择起点并单独核对。 |
| 动态发现分区 | 不指定分区时按原生 Doris 发现；默认 kafka_default_offsets 为 OFFSET_END，发现前已有消息可能被跳过。需要读取既有消息时，显式配置 `property.kafka_default_offsets=OFFSET_BEGINNING`，并核对实际范围与数据。 |
| 保留期/前缀删除 | 候选 next 小于 broker earliest 时，已删消息无法靠 offsets 恢复；不能依赖服务端一定 PAUSE/拒绝；当前组合实测出现零写入提交、游标推进到 end 而仍 RUNNING。显式改到 earliest 仍丢失过期区间，须从外部备份恢复或明确接受并记录损失。 |
| 日志尾部回退 | next 大于当前 end 时不能接续；即使后续 end 再增长到覆盖 next，旧数字也可能跳过占用复用 offset 的新消息。重置到 earliest 又可能重读已经写入 Doris 的保留前缀。仅检查 `[earliest,end]` 不足以证明历史未变。 |
| 同名 topic 删除重建 | cluster ID、名字和数字范围可以相同，topic ID 已不同。旧进度不能直接应用到新代际。 |

OFFSET_END/OFFSET_ZERO/OFFSET_BEGINNING 是未解析哨兵，不能直接加一；新分区、溢出、范围缺口、topic/cluster 代际、leader epoch 及截断记录无法确认时，保持隔离并人工核对数据，不猜测默认起点。Kafka 的分区数量不能通过普通增加分区接口缩小；删建 topic 是另一代际。检查也不是与后续消费原子绑定的屏障，维护期间必须禁止源重绑、截断、删除和其他入口改变准入条件。

下节的 Kafka 只读预检可以核对当前范围及先前基线，但不能认证 Doris 数据完整性；CLI 不自动迁移 offsets、重置到 earliest/latest、创建替代生产者或承诺端到端 exactly-once。Doris UUID 保留协议隔离名称，不校验 Kafka topic ID、分区集合、leader epoch 或日志内容。[Doris 创建参数](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/load-and-export/CREATE-ROUTINE-LOAD/)、[Kafka Admin API](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/admin/Admin.html)。

STOPPED 不回滚已提交数据，也不证明已获准的任务、事务和 BE 资源全部结束。继续 drain wait、复核事务/其他作业和数据后，才单独执行锁恢复；不自动接管。普通 drain 观察不要求保留视图，仍会阻塞未受管理的旧作业。

<a name="doc-preflight"></a>

## 记录与核对源历史

`kafka snapshot` / `kafka preflight` 直接读取 Kafka 公共 API，不需要 Java、Docker 或 Kafka 工具，也不连接 Doris。先在可信、已对账的时点保存基线，且在检查期间隔离源变更管理；接续时显式提供每个 partition 的候选 next offset。命令不读取消息、不加入消费组、不提交/重置 offset，不创建/停止生产者，不恢复迁移锁。Doris `Progress` 不会自动转换成输入。

连接配置放在外置盘的 0600 普通文件，CA、可选客户端 PEM 证书和未加密私钥同样要求外置、私密、无符号链接。示例为 SASL/PLAIN over TLS：

```json
{
  "format": "sqlx-doris-kafka-connection-v1",
  "bootstrap_servers": ["broker.example:9093"],
  "security_protocol": "SASL_SSL",
  "ca_file": "<external-volume>/kafka/ca.pem",
  "sasl_plain": {"username": "readonly", "password": "<set privately>"}
}
```

`SSL` 不带 sasl_plain；可选 client_certificate_file/client_key_file 必须成对提供。`PLAINTEXT` 不接受凭证或 TLS 文件。当前不支持 SCRAM、Kerberos、OAuth 或 SASL_PLAINTEXT；未知配置拒绝，不退回不验证的连接。TLS 始终检查 CA 与实际 broker 主机名，bootstrap 和所有 advertised broker 地址都须可达。账号只需要所用元数据 API 的读取权限，不授予主题/组修改权限。已实测 Kafka 4.0.0；不因 Kafka 预检增加 Doris 低版本适配。

将 KAFKA_CONFIG 设置为该私密 JSON 文件，并按安装说明配置 SQLX_DORIS_EXTERNAL_VOLUME。捕获新基线前，先创建外置 kafka 输出目录。

```sh
mkdir -p "$SQLX_DORIS_EXTERNAL_VOLUME/kafka"
cargo sqlx-doris admin kafka snapshot --connection-file "$KAFKA_CONFIG"   --topic events --output "$SQLX_DORIS_EXTERNAL_VOLUME/kafka/events-before.json" --json

# After STOP, transaction drain and business-data reconciliation; candidates are explicit.
cargo sqlx-doris admin kafka preflight --connection-file "$KAFKA_CONFIG"   --topic events --checkpoint "$SQLX_DORIS_EXTERNAL_VOLUME/kafka/events-before.json"   --offset 0=123 --offset 1=456 --timeout 30 --json
```

快照只写新的私密本地文件，保留 cluster ID、topic ID、完整分区集合、leader epoch、earliest/end 及 READ_UNCOMMITTED 语义。输出目录需预先存在；不覆盖已有文件。配置最大 64 KiB，快照/证书最大 1 MiB，最多 4096 分区、256 个 broker，响应帧上限 1 MiB；未知字段、重复分区、符号链接、宽权限、负数/哨兵/溢出 offset 都拒绝。topic ID 使用 UUID 十六进制形式，与 Kafka 工具的 Base64 文本是同一 16 字节的不同表示。

预检按 leader 批量读取范围与 OffsetForLeaderEpoch，复查元数据及范围是否在观察期间改变。它核对 cluster/topic 身份、完整分区集合和每个候选 offset 的 `[earliest,end]`；next=end 是有效的当前尾部位置。历史查询使用原快照的 epoch 和 end，能发现 tail 回退后又增长、旧 offset 重新落入范围的情况。缺少原基线、候选超出其历史覆盖、空基线、锚点已全部过期、epoch 证据缺失/不匹配都保持“不确定”。尤其 Kafka 可能回显已清理的请求 epoch，并返回较新 epoch 的起点；数字相等不作为完整历史证明。

| 退出码 | 含义 |
| --- | --- |
| 0 | 快照已保存，或本次读取与所给基线/候选一致；不是接续授权。 |
| 2 | 参数、私密输入、配置或快照格式无效。 |
| 3 | 已观察到身份/分区变化、范围冲突或历史尾部截断。 |
| 4 | 无法确认历史，或检查期间元数据/范围变化；保持隔离并对账。 |
| 7 | 连接、认证、权限、协议读取或总截止时间失败。 |

`--json` 可放在 kafka 后或叶子命令后；受控成功/错误只向 stdout 输出一个 `sqlx-doris-cli-result-v1` 对象，stderr 为空，help/version 保持文本。`checks` 给出各分区 range/history 与请求/返回 epoch，`issues` 提供稳定原因码。快照中的 `isolation_level` 固定为 read_uncommitted。所有结果均有 `server_read_only:true`、`atomic_snapshot:false`、`kafka_continuation_verified:false`、`safe_to_recover:false`，不含密码、连接配置或原始服务端错误。强杀/崩溃不能保证完整 JSON。

**退出 0 不证明无损或无重复。** 多次读取不是原子快照，检查之后也可能改变；必须另行约束源管理。基线文件没有服务端签名，必须由管理员可信保存，不能把编辑后的内容当作历史证据。API 只暴露保留 epoch 的边界，不提供完整截断审计；基线以前的截断、元数据/ID/epoch 恢复或重用、日志压缩/区间内消息缺口、消息内容、Kafka 事务的 READ_COMMITTED 可见性，以及 Doris 实际消费/写入均未被认证。当前范围使用 READ_UNCOMMITTED，不能据此推断每条消息存在或可见。事后新建快照不能补出丢失的旧历史，不要用新基线掩盖旧预检冲突；先核对数据和来源，再由管理员决定新的已知边界。参见 [Kafka 协议](https://kafka.apache.org/40/design/protocol/) 与 [Kafka epoch 保留行为](https://github.com/apache/kafka/blob/4.0.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java)。
