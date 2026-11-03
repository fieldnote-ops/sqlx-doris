<a name="doc-top"></a>

# 受管理 Routine Load

[English](ROUTINE_LOAD.md) | [简体中文](ROUTINE_LOAD.zh-CN.md)

[文档首页](README.zh-CN.md)

高级选用功能：安装 `sqlx-doris` 的 `cli-admin` 特性；命令位于 `cargo sqlx-doris admin` 下。日常查询和 Stream Load 不要求此功能。

用于已验证拓扑上的新建单表 Kafka 作业，不收编普通旧作业。显式 STOP 前按[恢复流程](RECOVERY.zh-CN.md)退役创建者并核对工作；Progress 与 offset 解释见 [Kafka 接续](KAFKA.zh-CN.md#doc-progress)。

<a name="doc-contents"></a>

## 目录

- [创建、检查与显式停止](#doc-protocol)
- [回执、状态与退出码](#doc-receipts)
- [不复用边界](#doc-non-reuse)

<a name="doc-protocol"></a>

## 创建、检查与显式停止

**新创建的作业可以使用 routine create/status/stop，但安全依赖所有创建者共同遵守不复用协议。** 仅支持已验证的 4.1.5、单 master FE、内置认证/授权、shared-nothing、当前 internal 数据库的单表 Kafka 作业。独立管理员 URL 保持在管理员 CLI；`--runner-identity-file` 使用 identity create 生成的现存私密外置凭证，CLI 另开受限连接提交 Routine CREATE，实际作业的 User 必须等于该 runner。管理 URL 与 runner 文件必须对应同一端点、数据库及连接选项。全局 --identity-file 不适用于 routine 管理。

create 成功仅确认创建与绑定，不等于已开始消费或导入成功，实际运行状态仍要检查。每次创建生成新的 `sqlx_rl_<UUID v4>`，不接受自选名字、旧名或收编现有作业。先同步一个新建的 0600 外置 receipt，再以不带 IF NOT EXISTS/OR REPLACE 的原子 CREATE VIEW 保留 `_sqlx_rl_<UUID>`；保留记录包含协议、cluster/database ID、目标表、创建账号与名字。只有收到保留成功确认并核对记录后，才提交**一次** Routine CREATE。这个普通视图每代永久保留，不存数据、不建 tablet，但占用 FE 元数据；禁止改写、删除、恢复旧记录或将它当作可清理缓存。创建失败、断连或取消也消耗该名字，不能重放 CREATE；保留记录同时阻止遵守协议的创建者在 UUID 碰撞时复用名字。

准备外置 SQL 后缀文件（从 `ON <table>` 后开始；包括 FROM KAFKA，最多 64 KiB UTF-8，任何位置都不能有分号或 NUL）：

```sql
COLUMNS TERMINATED BY ',', COLUMNS(id,value)
PROPERTIES('desired_concurrent_number'='1','max_batch_interval'='5')
FROM KAFKA('kafka_broker_list'='kafka:9092','kafka_topic'='events',
           'kafka_partitions'='0','kafka_offsets'='OFFSET_END')
```

目标表由应用事先创建。沿用前述管理员环境和尚未退役的 `$credentials`，receipt 的父目录需已存在：

```sh
cargo sqlx-doris admin routine create --runner-identity-file "$credentials" --table events \
  --source "$SQLX_DORIS_EXTERNAL_VOLUME/routine.sql" \
  --output "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --managed-namespace
# Receipt selects its database and verifies cluster/database IDs with independent admin credentials.
cargo sqlx-doris admin routine status --receipt "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --json
# Alternatively use --job-name in the administrator URL database.
# Isolate old actors/restart paths and retire the reported creator:
cargo sqlx-doris admin identity retire --name 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
cargo sqlx-doris admin routine stop --receipt "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --json
cargo sqlx-doris admin routine stop --receipt "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --job-id 12345 \
  --maintenance --managed-namespace --force --json
```

stop 默认只读预览，可不传 job-id 来发现当前精确 ID；force 必须显式传入 job-id、maintenance 和 managed-namespace。--receipt 与 --job-name 二选一，receipt 仅用于定位和核验，不授予停止权限。它验证永久视图、cluster/database、唯一保留历史、创建账号、目标表、Kafka 来源及精确 JobId/创建时间；创建账号必须已经退役，提交前再核对一次，只发送一次 UUID 名字 STOP。任意 UUID 外形不足以获得管理资格；不存在记录、未知格式、错误 ID、复用历史、改名目标或已终态均拒绝。当前命令不能收编旧普通作业，也不做 rename。成功还需核验同一作业 STOPPED，记录、双锁和 dirty/applied 历史均保留。STOP、创建和元数据读取共享全局 --ddl-timeout 总截止时间。

<a name="doc-receipts"></a>

## 回执、状态与退出码

create 的主 receipt 在服务端 mutation 前以新建文件写入并同步，不覆盖已有文件；成功核验作业后再新建 `<receipt>.confirmed`，记录 JobId、创建时间和当时的状态/Progress。两份文件各限 1 MiB，必须是外置盘的私密普通文件（0600），不接受符号链接、宽权限、未知格式、额外字段或不匹配的 cluster/database/reservation/job 绑定。主文件不含密码或 SQL；确认文件是**历史检查点**，不能替代当前服务端核验。只有主 receipt、尚未写出确认文件时仍可检查；删除确认文件会丢失本地历史证据，绝不能因此推断没有作业。

routine create/status/stop 都支持 --json（可放在 routine 后或叶子命令后）。受控成功和错误只向 stdout 写一个完整 JSON 对象，stderr 为空，schema 为 `sqlx-doris-cli-result-v1`；help/version 保持标准文本，信号强杀和进程崩溃不能保证输出 JSON。原始 SQL、Kafka 配置和服务端错误不回显，避免泄露凭证。所有命名空间均使用同一全局协议，见 [CLI 结果](CLI.zh-CN.md#doc-results)。

`observation` 区分 active（包括 PAUSED）、terminal（STOPPED/CANCELLED）、reservation_missing、reserved_unobserved、history_unavailable、invalid_binding、unconfirmed 和 unknown；`outcome` 区分 observed/preview/created/stopped 与 invalid_input/refused/inconclusive/uncertain/acknowledged_unverified/unavailable。主 receipt 有保留但没有可见作业时返回 reserved_unobserved；确认文件已记录过 JobId 而当前不可见时返回 history_unavailable。**两者都不能判定 never_created 或排空成功**：CREATE 可能晚到，历史可能被清理或隐藏。

`phase` 使用统一阶段；Routine mutation 的 `step` 标明 receipt_writing、receipt_persisted、reservation/create/stop 的 submitted/acknowledged 阶段及 create_verified/stop_verified。`receipt_output` 保留请求路径和最后的持久化检查点；writing 不证明文件已经保存，只有 persisted 表示已完成文件与父目录同步。文件创建前的输入错误返回 2，写入或同步失败返回 7，均不继续服务端 mutation。`job` 是最近一次成功读取的快照，其读取阶段由 job_observed_phase 标识；后续 mutation 未确认时它不代表现在的状态。`last_confirmed_job` 仅来自本地历史确认文件。JSON 总是带 `safe_to_recover:false`、`automatic_retry:false`，终态也必须另做 drain 与数据核对。

| 退出码（routine） | 含义与下一步 |
| --- | --- |
| 0 | 只读观察/预览成功，或创建/停止已核验；不代表可恢复锁。 |
| 2 | 参数、URL、receipt/源文件或本地输入不合法；未发服务端 mutation。 |
| 3 | 安全条件或代际绑定不满足、创建者未退役、错误 ID、重复停止终态；未发本次 STOP。 |
| 4 | 元数据快照无法确认：缺保留、作业未出现或历史不可见；保留证据，不推断完成。 |
| 5 | mutation 可能已提交但没有成功确认；查原 receipt/名字/ID，绝不重放。 |
| 6 | mutation 已收到 ACK，但后续核验或确认文件失败；用新连接检查，绝不重放。 |
| 7 | mutation 提交前的连接/读取失败或超时；保持隔离并重新观察。 |

提交后的错误优先归为 5/6，不会被后续读取或本地文件错误降为普通输入错误。恢复工具应依据 exit_code、outcome、phase、step 和快照阶段一起处理；不要循环执行 force，也不要把非零统一当作“未执行”。

<a name="doc-non-reuse"></a>

## 不复用边界

在该协议内，后继必须重新 create，不能通过 ALTER/RESUME 将旧 UUID 作业交给新实例。后继创建总使用另一个 UUID，迟到的旧 STOP 只能查旧名；断连、CLI 被终止和超时取消后的真实测试均证明后继保持运行。无回执报告结果不确定，收到 ACK 后读取失败单独报告；用新连接查原始名字/ID，不重发 STOP。CREATE 结果不明时查原始 receipt：暂时没有作业不能证明排队 CREATE 不会晚到；不要先忽略它再创建替代作业，否则可能出现两个生产者。新 UUID 只解决名称误停止，不提供数据去重或 Kafka offset 接续。

**Doris 并不在 Routine CREATE 中检查本库的保留视图。** `--managed-namespace` 是所有创建者、运维工具和管理员共同独占 `sqlx_rl_`/`_sqlx_rl_` 命名空间的承诺，不是服务器 ACL。特权 SQL 绕过协议复用旧名时，迟到 STOP 仍会误停止该作业；原生负面测试固定了这一边界。因此必须禁止绕过、删除保留记录、同名数据库重建、元数据回滚/恢复、ID/名字复用以及操作期间端点切换。无法保证这些条件时，不使用 force，继续检查/等待并采用外部隔离。没有自动修改服务端准入配置。
