<a name="doc-top"></a>

# CLI 参考与诊断

[English](CLI.md) | [简体中文](CLI.zh-CN.md)

[文档首页](README.zh-CN.md)

按[安装说明](INSTALLATION.zh-CN.md#doc-cli)通过公开包安装。命令使用 DATABASE_URL 或 --database-url；受管理 runner 改用 --identity-file（需要 cli-admin），并清除管理员 URL。完整参数见 `cargo sqlx-doris <command> --help`。

<a name="doc-contents"></a>

## 目录

- [选择命令](#doc-commands)
- [JSON、状态与稳定退出码](#doc-results)
- [异常恢复只读诊断](#doc-diagnose)
- [诊断证据与原因码](#doc-reasons)

基础 cli 安装只包含 prepare、database 和普通 migrate。cli-admin 开启 admin lock/identity/drain/routine/diagnose 和 admin migrate repair；cli-admin-kafka 再开启 admin kafka 和诊断中的 Kafka 参数。未启用功能不会出现在帮助中，不保留旧顶层命令别名。

<a name="doc-commands"></a>

## 选择命令

| 命令 | 用途 |
| --- | --- |
| `prepare` | [编译期查询元数据与离线缓存](USAGE.zh-CN.md#doc-offline) |
| `database`, `migrate` | [数据库生命周期与迁移文件](MIGRATIONS.zh-CN.md) |
| `admin lock`, `admin identity`, `admin drain` | [显式恢复与限定范围观察](RECOVERY.zh-CN.md) |
| `admin routine` | [永久 UUID Routine Load 协议](ROUTINE_LOAD.zh-CN.md) |
| `admin kafka` | [源接续只读预检](KAFKA.zh-CN.md) |
| `admin diagnose` | 下文的只读组合诊断 |

<a name="doc-results"></a>

## JSON、状态与稳定退出码

所有命令支持全局 `--json`，可放在子命令前或后（prepare 的 Cargo `--` 分隔符之前）。受控成功和失败均只向 stdout 写一个 `sqlx-doris-cli-result-v1` 对象，stderr 为空；help/version 保持文本。强杀、崩溃或 stdout 写入失败不能保证完整 JSON。

人类输出在 stderr 以同一套 status/phase/step/exit-code 收尾；需要解析 owner 或状态的脚本使用 JSON。`failure_category` 保留阶段优先前的错误分类，无法细分时为 unavailable。`work` 仅包含已观察到的条目，空对象不证明排空。

公共字段为 `operation`、`exit_code`、`status`、`phase`、`step`、`issues`、`message`、`next_actions`；其余字段保留具体观察：lock 的 `locks/recovery_drop`，identity 的 `identity`，drain 的 `work/drain/target`，migrate info 的 `migrations`，prepare 的 `prepare/cargo_diagnostics_file`，Routine 的 receipt/job，以及 Kafka 的 checkpoint/checks。JSON 不回显原始 SQL、服务端错误或凭证。prepare 在私密外置文件保存 Cargo 诊断，JSON 仅返回定位路径。

| 退出码 | 公共含义 |
| --- | --- |
| 0 | 本次命令在所述范围内完成；不代表排空、无损接续或可以接管。 |
| 2 | 参数、URL、输入或配置不合法。 |
| 3 | 已观察到阻塞/绑定/校验冲突，或 Cargo 编译检查失败。 |
| 4 | 无法确认、缺少核验范围，或观察期间状态变化。 |
| 5 | 服务端修改可能已提交，尚无成功确认；检查原对象，禁止盲目重放。 |
| 6 | 修改已确认，但后续步骤、核验或收尾未完成。 |
| 7 | 连接、权限、元数据读取、截止时间或本地操作失败。 |

`phase` 统一为 `read_only/local_write/submitted/acknowledged/verified`，`step` 标明具体步骤，例如 `stop_submitted`。提交后的阶段优先于后续输入/读取错误；多步骤迁移可能已有部分 SQL/协调操作完成，5/6 不是整份文件回滚结论。`local_write` 只描述本地文件操作，错误不归为服务端提交不确定。`server_read_only` 表示本次未进入服务端持久修改阶段，连接初始化的会话设置仍会发生。主锁恢复还可能修改协调元数据；`recovery_drop.phase=not_submitted` 只确认尚未提交 owner 分区 DROP，不声称协调表完全未变。该字段来自驱动的明确阶段，不通过解析错误文字推断。

所有结果均有 `safe_to_recover:false`、`atomic_snapshot:false` 和禁止自动命令重放/offset 转移/重置的字段。`automatic_retry:false` 针对命令重放和不确定修改；只读轮询、bootstrap 连接回退及已确认互斥冲突下的锁等待仍按各命令约定执行。Kafka 原先的细分状态位于 `observation`，公共 `status` 使用上述统一分类。`migrate info` 查询空库时不再创建状态表；已应用、dirty 等历史位于 migrations，源文件对应状态仍可在人类输出中查看。

`migrate` 与 `diagnose --table` 共用驱动的迁移表名规则：支持带引号和当前数据库限定的名称（例如 ``--table '`app`.`schema_history`'``），拒绝跨库、SQL 片段与保留锁表。

<a name="doc-diagnose"></a>

## 异常恢复只读诊断

`diagnose` 串联锁 owner、迁移历史、可选源文件 checksum、退役身份/会话、全库事务与限定异步作业，以及可选 receipt 和 Kafka 预检。它不创建状态表，不退役账号、停止作业、修复历史、清锁或替换作业。仅使用独立管理员 URL，拒绝 `--identity-file`、`--force`、`--maintenance` 等修改参数。

复制示例前，将 OLD_ID 设置为已核验的旧身份，RECEIPT 指向原始私密回执，KAFKA_CONFIG 指向私密 Kafka 配置，BEFORE 指向可信的早期检查点，REPORT 指向新的外置报告路径。只提供需要检查的可选参数组；offset 是明确核对后的候选值。

```sh
cargo sqlx-doris admin diagnose --retired-identity "$OLD_ID" --source migrations --json
cargo sqlx-doris admin diagnose --receipt "$RECEIPT" --retired-identity "$OLD_ID" --json
cargo sqlx-doris admin diagnose --receipt "$RECEIPT" --retired-identity "$OLD_ID" \
  --connection-file "$KAFKA_CONFIG" --topic events --checkpoint "$BEFORE" \
  --offset 0=123 --offset 1=456 --timeout 30 --json
```

receipt 选择数据库并复用原有 cluster/database/reservation/JobId 校验；提供的退役身份若与已核验创建者不同，会报告冲突。`components` 分别保存退出码、观察内容和记录时间；`coverage` 标明实际请求的检查。Doris 不可达时，独立 Kafka 组件仍可检查（总截止时间耗尽则未执行的组件未知）。汇总退出码优先级为 2、7、3、4、0，已知冲突保留在各组件，不会被汇总的不可用状态隐藏。因前置失败或超时未读取的组件明确标为 not_observed。

未提供身份返回 4，因为旧凭证和会话未核验。身份仍存在、非终态工作、dirty 或源 checksum/缺失冲突返回 3。缺失源文件、receipt 或 Kafka 参数组的可选范围不会被擅自补齐；coverage 明确标记。锁被持有仅报告 owner，诊断不会尝试获取或释放它。检查前后若锁或历史变化，返回 4；超时或读取失败保留部分结果并返回 7。

**诊断退出 0 也不是恢复授权。** 不同组件顺序读取，时间戳不组成原子快照；仍需外部隔离旧执行者/准入，并对账已提交数据和未覆盖工作。Kafka 配置是独立输入，目前不认证其 broker/topic 与 Routine 实际来源的绑定，`kafka_source_bound_to_routine:false`；候选 offset 始终由管理员确认。完整历史、消息内容、业务数据一致性与完全后台排空仍未被认证。实际恢复命令继续执行自己的新鲜检查，不消费此报告作为令牌。

<a name="doc-reasons"></a>

## 诊断证据与原因码

人类输出显示 dirty 版本、checksum 差异、组件状态和处理建议。观察阶段抑制子命令进度；服务端文本按 JSON 转义，最多展开 20 个活动工作项。完整工作列表仍在 JSON 的 `work`。迁移组件的 `observation.pending_versions` 表示源中尚无历史记录的 up 版本，不表示已经执行。

完成诊断流程后，`issues` 条目包含 `code/component/exit_code/status/message/context/next_action`，并出现在所属组件的 `issues` 中；同时存在的原因分别保留。参数验证错误可能只有 `code/message`。`read_only_commands` 给出不含 URL/凭证的 argv 建议，须复用原管理员连接，且不会自动执行；非 UTF-8 路径的建议省略。

| 稳定原因码 | 含义与处理 |
| --- | --- |
| `migration_dirty` | `context.version` 为 dirty 版本，先核对实际 schema 与已提交数据 |
| `migration_checksum_mismatch` / `migration_source_missing` | 对照记录及源 checksum，或找回原 up 文件；保留历史 |
| `migration_history_invalid` / `migration_table_ambiguous` | 历史摘要、版本或表身份无法确认，保留现场 |
| `migration_table_invalid` / `migration_source_invalid` | 修正表名、源目录或重复 up 版本 |
| `identity_present` / `identity_not_supplied` | 身份仍存在或未提供；不证明身份到锁的归属 |
| `work_active` | 检查已观察事务、会话及异步作业的终态；空列表不补足隐藏工作 |
| `lock_held` / `migration_table_absent` | 退出码 0 的信息项：有 owner，或状态表不存在且未创建 |
| `observation_changed` | owner 或历史在观察期间变化，先外部停止并发管理者再只读复查 |
| `routine_active` / `routine_unverified` / `routine_identity_mismatch` | 作业未终止、绑定未确认或身份不匹配，检查原 receipt，不重复 CREATE/STOP |
| `kafka_preflight_failed` | 分区原因见 Kafka 组件的 `observation.issues/checks`，保留候选 offset 与更早基线 |
| `diagnosis_deadline_expired` / `component_not_observed` | 截止时间或前置失败导致检查未完成，部分证据保留 |
| `database_unavailable` / `lock_metadata_unavailable` / `lock_recheck_unavailable` / `migration_history_unavailable` / `migration_history_recheck_unavailable` / `drain_unavailable` | 对应连接或元数据阶段未完成；检查端点、TLS、权限和支持的拓扑 |
| `diagnosis_input_invalid` / `administrator_connection_required` / `identity_input_invalid` / `receipt_input_invalid` / `receipt_unavailable` | 修正输入，或检查原私密 receipt、权限及外置盘可用性 |

原因来自检查阶段或类型化错误，不匹配服务端错误消息。`issues` 可包含退出码为 0 的信息项；脚本以进程及每项退出码判断，不以数组是否为空判断。

```sh
diagnose_exit=0
cargo sqlx-doris admin diagnose --receipt "$RECEIPT" --retired-identity "$OLD_ID" --json > "$REPORT" || diagnose_exit=$?
jq '.issues[] | {component, code, exit_code, context, next_action}' "$REPORT"
jq '.read_only_commands' "$REPORT"
```
