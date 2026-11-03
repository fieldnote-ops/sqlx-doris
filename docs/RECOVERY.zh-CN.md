<a name="doc-top"></a>

# 迁移异常恢复流程

[English](RECOVERY.md) | [简体中文](RECOVERY.zh-CN.md)

[文档首页](README.zh-CN.md)

高级选用功能：安装 `sqlx-doris` 的 `cli-admin` 特性；命令位于 `cargo sqlx-doris admin` 下。日常查询和 Stream Load 不要求此功能。

使用独立管理员连接，开始前停止新部署。诊断结果只是观察，不是恢复许可。旧迁移、解锁、恢复执行者及重启入口必须持续停止；跨机器管理员操作需串行。

<a name="doc-contents"></a>

## 目录

- [选择下一步](#doc-sequence)
- [专属迁移身份](#doc-identity)
- [观察与等待排空](#doc-drain)
- [LOAD 终止边界](#doc-load)
- [MV 任务取消与调度](#doc-mv-tasks)
- [恢复精确 owner 并修复历史](#doc-locks)
- [隔离与覆盖边界](#doc-limits)

<a name="doc-sequence"></a>

## 选择下一步

1. 运行[只读 diagnose](CLI.zh-CN.md#doc-diagnose)，保留原始回执、owner 和历史。
2. 关联旧 runner，隔离进程、重启入口和专属凭证。
3. 按下文退役、排空观察，并核对真实数据、schema、已准入语句及未覆盖作业。
4. 若转换门仍持有，先预览；完成核对后才显式删除精确 owner，再查看实际主锁 owner。
5. 显式恢复当前主锁；必要时修复已核对的 dirty 版本。
6. 启动新 runner 前再次验证；不盲目重试 CREATE/STOP/DROP 或不确定的迁移 SQL。

`lock status` 只读。**只有 recover-transition 提供预览；主锁 lock recover 是修改操作，`--force` 就是执行确认。** 本流程统一提供 `--maintenance`；使用 `--retired-identity` 时 CLI 也要求该参数。**不要用 --force 做检查。** 删除账号、收到 STOP 回执或工作列表为空，都不能证明旧写入已全部结束。

<a name="doc-identity"></a>

## 专属迁移身份

内置 RBAC 下，可以用一次性独立账号撤销旧 runner 的后续认证与权限。CLI 提供 identity create/status/retire；当前只自动处理 **Doris 4.1.5、单个存活 master FE、内置 authentication_type=default / access_controller_type=default**。多 FE 的权限传播、LDAP/Ranger/外部鉴权以及未知版本没有得到全节点验证，命令会在变更前拒绝；不能据此宣称这些环境已经隔离。

identity create 生成不复用的 `sqlx_mig_<UUID>`@'%'，只直接授予当前 internal 数据库的 SELECT、LOAD、CREATE、ALTER、DROP，不授予 ADMIN/GRANT。管理员凭证与 runner 进程/权限应分离，旧实例不得拥有其他可写身份或备用管理员秘密。账号按 runner/部署实例独占，后继实例必须新建身份；不要共享凭证或重新创建/授予已退役的名字。此账号不能用于跨数据库、外部 Catalog 或需要额外权限的迁移。

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
export DATABASE_URL='doris://administrator:password@doris-fe:9030/app'
credentials="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/identities/deployment.json"
cargo sqlx-doris admin identity create --output "$credentials"
env -u DATABASE_URL -u DORIS_TEST_URL cargo sqlx-doris --identity-file "$credentials" migrate --source migrations run
# Runner environments must contain no administrator secrets or fallback URLs.
cargo sqlx-doris admin identity status --name 'sqlx_mig_<32-hex-digit UUID>'
cargo sqlx-doris admin identity retire --name 'sqlx_mig_<32-hex-digit UUID>'
cargo sqlx-doris admin identity retire --name 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
cargo sqlx-doris admin lock status
```

该流程使用独立密码身份，不继承管理员的 TLS 客户端证书/密钥；新建和读取 runner 文件时遇到 ssl-cert/ssl-key 会拒绝，TLS CA 与服务器身份校验可保留。凭证 JSON 含密码，严格放外置盘，Unix 权限 0600；不覆盖已有文件，拒绝符号链接、盘外路径和宽松权限，输出不打印密码。可供应用读取 database_url，再按原生 SQLx/DorisConnectOptions 连接。文件先写入并同步，再创建账号/授予权限；中断可能留下尚未创建或只完成部分授权的文件/账号。先查 status，再显式处理，不自动重试 CREATE/GRANT/DROP；不要把凭证文件提交仓库或作为普通日志保存。身份管理自身禁止 --identity-file，使用单独的管理员 URL。

开始隔离前关闭新部署/runner 入口，维护期间保持关闭。retire 默认只读预览，--force 必须附带 --maintenance。它检查名字格式、当前数据库/cluster 的管理标记、唯一 @'%' identity 和权限布局；遇到其他 host identity、角色、全局/其他数据库权限、证书约束、未知权限格式或不匹配标记会拒绝，不自动撤销任意共享账号的权限。维护期间禁止其他管理员改权、重建名字或复用身份；DROP USER 没有按 account generation 比较删除的接口。预览显示当前双锁 owner，但不是账号与锁的归属证明，必须按部署日志核对选中的旧实例。

验证通过后，retire 执行单次 DROP USER，并在同一受验证 FE 核验名字不存在；已有会话后续普通和缓存预编译写入被拒绝，旧凭证不能重连。只 REVOKE 直接 LOAD 权限不能阻止登录，角色权限仍可授权，因此不把一次 REVOKE 当作隔离完成。DROP 已确认但后续账号/环境核验失败会单独报错；元数据 ACK 丢失会报告结果不确定；用新连接查 status，不重建或盲目重试这个名字。不存在的名字不被当成可重放成功。

**账号退役不清除任何主锁、转换锁或 dirty/applied 历史，也不表示旧操作已排空。** 已在权限检查前获准的语句可能完成；账号删除前已经写入的事务，在删除后 COMMIT 仍可能成功，已提交 ALTER 也会继续执行。这些边界已在真实 Doris 验证。退役后仍要停止旧进程/重启入口，核对并结束事务、COLUMN/ROLLUP 作业及真实数据，再单独执行 lock recover-transition / lock recover 和 dirty 修复；不自动抢锁、取消事务或重放迁移。

官方语义见 [内置授权](https://doris.apache.org/docs/4.x/admin-manual/auth/authorization/internal/)、[REVOKE](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/account-management/REVOKE-FROM/) 和 [DROP USER](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/account-management/DROP-USER/)。

<a name="doc-drain"></a>

## 观察与等待排空

`drain status/wait` 使用独立管理员连接，沿用身份退役的单 FE、Doris 4.1.5、内置鉴权限制，并要求存算一体（deploy_mode 为空/ local、cloud_unique_id 为空）；未知或存算分离部署拒绝通过。排空与 CLI 恢复当前要求非系统 ASCII 数据库标识符。名字必须来自该数据库旧部署的私密凭证/部署日志；账号已不存在并不能证明其历史归属。命令要求该名字在当前 FE 已不存在，拒绝重新创建或权限布局异常的身份。检查范围如下：

| 检查对象 | 阻塞及终态 |
| --- | --- |
| 旧身份的全部 PROCESSLIST 会话 | 包括空闲、切换数据库后的会话；仍存在就阻塞。CLI 不根据数字 ID 自动 KILL，请从旧进程/部署入口关闭连接。 |
| 当前数据库全部运行事务 | 元数据没有可靠的用户归属，因此其他账号的事务也会阻塞；PREPARE、PRECOMMITTED、COMMITTED 均未排空，仅 VISIBLE/ABORTED 是终态。不会按猜测归属自动回滚；仅支持下文显式绑定 BROKER LOAD 的事务 ID abort。 |
| 当前数据库 COLUMN/ROLLUP 作业 | 等待 FINISHED/CANCELLED。 |
| SHOW LOAD 可见的当前数据库作业 | PENDING/ETL/LOADING/COMMITTED 等阻塞；FINISHED/CANCELLED 为终态。另查旧 LOAD 注册表 pending/loading 计数，避免仅凭渲染失败的空列表通过。 |
| SHOW ALL ROUTINE LOAD | NEED_SCHEDULE、RUNNING、PAUSED 均阻塞，仅 STOPPED/CANCELLED 为终态；PAUSED 可恢复，已分派任务不会因此全部停止。 |
| 异步 MV 调度器及任务 | 将 mv_infos、jobs('type'='mv') 和 tasks('type'='mv') 按数据库、MvId/JobId 配对。ON COMMIT 和定时刷新处于 RUNNING 时，即使没有任务也阻塞；不能只看 ExecuteType=MANUAL，因为 ON COMMIT 也用该值。手动刷新且无未完成任务，或调度器已 PAUSED/STOPPED/FINISHED，才视为本次调度观察无阻塞。任务仅 SUCCESS/FAILED/CANCELED 为终态。 |

等待期间已观察到的事务/作业必须读到精确终态或调度器停用状态；记录消失、代际/目标变化、未知状态、事务列表达到 2000 行上限、断连及权限错误均不能通过。任务/生产者检查后再次读取运行事务。MV 的 FAILED/CANCELED 并不代表迁移成功，也不撤销已提交分区；dirty 记录保持不变。

status 发现阻塞返回非零；wait 每秒检查，`--timeout` 是包含连接与元数据读取的总秒数，超时返回非零。status、cancel-load、cancel-rollup、cancel-mv-task 及恢复预检使用全局 `--ddl-timeout`（默认 300 秒）。两者不清锁、不改 dirty、不重放业务 SQL。数据库/cluster/topology/身份在检查期间变化也会拒绝。即使所有列表为空，也只表示这些接口在读取时未发现阻塞，不能证明此前获准而尚未登记的语句、其他作业类别或后续写入已被服务端原子隔离。wait 的 `--maintenance` 表示操作者已停掉旧 runner、重启/连接入口和其他写入者，串行化管理员操作，并核对其他异步工作；它不能替代这些操作。

```sh
# Stop old actors/restarts/admission and explicitly retire first; never reuse/regrant the name.
cargo sqlx-doris admin drain status --name 'sqlx_mig_<32-hex-digit UUID>'
cargo sqlx-doris admin drain wait --name 'sqlx_mig_<32-hex-digit UUID>' --timeout 300 --maintenance
# Separately reconcile data/schema, Stream Load/COPY, generic scheduled INSERT,
# EXPORT, backup/restore, index-build and other uncovered work.
cargo sqlx-doris admin lock recover-transition --owner '<gate UUID>' --lock-owner '<main UUID or unlocked>' --retired-identity 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
cargo sqlx-doris admin lock recover --owner '<main UUID>' --retired-identity 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
```

所有 CLI lock recover / recover-transition 都会重新检查当前数据库事务及上述作业；缺失权限、未知部署、元数据错误或阻塞均会在恢复 DROP 前停止，转换锁预览也执行该检查。`--retired-identity` 额外验证名字已退役及旧身份会话已消失；不提供时不会认证凭证/会话隔离，CLI 会明确提示。之前 wait 的输出不是可复用的接管凭证，不自动调用 wait/retire，也不自动清锁。最终快照与 DROP 之间没有服务端事务屏障，维护条件必须持续成立。驱动层恢复 API 仍是要求外部排空的显式维护原语，不承诺 CLI 的作业诊断。

对于确实属于旧部署、仍在运行的 ROLLUP，可显式预览和取消单个 JobId：

```sh
cargo sqlx-doris admin drain cancel-rollup --name 'sqlx_mig_<32-hex-digit UUID>' --table events --job-id 12345
cargo sqlx-doris admin drain cancel-rollup --name 'sqlx_mig_<32-hex-digit UUID>' --table events --job-id 12345 --maintenance --force
```

只发送一次包含精确 JobId 的 CANCEL，并轮询该作业至终态；缺失、表名不匹配、已终态会在发送前拒绝，重复旧 ID 不会取消同表后继作业。取消结果可能是 FINISHED，表示作业抢先完成，不能当作回滚成功；仍须核对真实 schema。ACK 丢失、取消进程或超时后，已提交 CANCEL 仍可能完成；用新连接检查，不盲目重试。取消本身不表示事务/数据库已排空。

<a name="doc-load"></a>

## LOAD 终止边界

**已登记事务的 BROKER LOAD 可以按精确事务终止**，不使用按 label 选择的 CANCEL LOAD。先查预览，再使用预览中的同一对 JobId/TransactionId 执行：

```bash
export DORIS_HTTP_URL='http://doris-fe:8030'
cargo sqlx-doris admin drain cancel-load --name 'sqlx_mig_<32-hex-digit UUID>' --job-id 12345
cargo sqlx-doris admin drain cancel-load --name 'sqlx_mig_<32-hex-digit UUID>' --job-id 12345 --transaction-id 67890 --maintenance --force
```

只读预览不要求 HTTP 地址；执行时通过 DORIS_HTTP_URL 或显式 `--http-url` 提供。执行前要求 BROKER、PENDING/LOADING、正 TransactionId，且 SHOW TRANSACTION 的 ListenerId=JobId、label 相同、来源 BATCH_LOAD_JOB、状态 PREPARE/PRECOMMITTED；发送前再次核对。CLI 调用公开 `_stream_load_2pc` 的 `txn_id`/`txn_operation:abort`，不传 label、不跟随重定向、不重试。4.1.5 的事务管理器在锁内按精确事务 ID 转换状态，随后回调对应 LOAD；最后一次检查后即使 CLEAN LABEL 并创建同名新作业，旧事务请求也不会选择后继。若提交先完成，abort 会被拒绝，已提交数据保留。成功还须核验同一事务 ABORTED、同一作业 CANCELLED。[事务接口](https://doris.apache.org/docs/4.x/data-operate/transaction/)、[4.1.5 事务实现](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java)、[LOAD 回调](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadJob.java)。BROKER 用法依赖此版本实现并经真实测试，不能外推到其他版本。

该路径目前仅支持内置认证/授权、shared-nothing 的单 master FE、单 live BE、Doris 4.1.5。使用独立 SQL 管理员 URL 中的密码认证；不使用 StreamLoadClient 的 DORIS_HTTP_USER/PASSWORD，也不继承 SQL 客户端证书/私钥。FE origin 不得含凭证/路径/查询参数；HTTP 节点信息须与 SQL 的 FE 名字、ClusterId、端口、版本及启动时间等相符；BE 必须是 SHOW BACKENDS 中的登记节点。重新检查节点及作业绑定后只发送一次 BE PUT，丢弃 Location 内凭证并独立认证。网络必须能访问该 BE 的登记地址；不提供任意 BE 覆盖。要求 TLS 的 SQL URL 也要求 HTTPS，`ssl-ca` 用于 HTTP CA 校验，不提供关闭 HTTP 证书验证。容器/代理部署须提供可信、稳定的路由。

`--maintenance` 还确认没有集群切换、地址重绑定、元数据恢复或 ID 复用；HTTP 身份读取不是与随后的 abort 原子绑定的集群证明，无法抵御检查后端点被替换。超时、断连或 CLI 退出后已提交请求仍可能生效；无回执与回执成功后核验失败分别报错，用新连接核对原始 ID，不能改用名称或重复发送。命令不动锁/dirty 历史；事务终态不证明所有 BE 资源已回收，仍须 drain wait、核对其他任务及数据，再单独恢复锁。

尚未登记事务的 LOAD（TransactionId=0）、SPARK/INSERT、普通 SQL/Stream Load 事务以及其他来源拒绝显式 abort，不等待并自动绑定后续事务。普通或手工创建的 ROUTINE LOAD 仍只检查/等待：STOP 按名称解析，没有 JobId 条件或可用的原子 rename；先检查 ID 仍会误停止换代后的同名作业。全局配额也不是代际锁，`max_routine_load_job_num=0` 的实现使用 `count > limit`，不禁止零作业时的首个。CREATE 与 STOP 都使用表 LOAD 权限，不能把撤销一项权限当成只禁创建、保留停止；CLI 不修改全局配额来伪造准入隔离。[STOP 语法与权限](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/load-and-export/STOP-ROUTINE-LOAD/)、[4.1.5 管理器](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadManager.java)。

<a name="doc-mv-tasks"></a>

## MV 任务取消与调度

异步 MV 可显式取消某一代视图的一个任务：

```sh
cargo sqlx-doris admin drain cancel-mv-task --name 'sqlx_mig_<32-hex-digit UUID>' --view summary --job-id 12345 --task-id 67890
cargo sqlx-doris admin drain cancel-mv-task --name 'sqlx_mig_<32-hex-digit UUID>' --view summary --job-id 12345 --task-id 67890 --maintenance --force
```

从 drain 阻塞输出可获得 MV job <JobId> task <TaskId>，view 取该目标视图名（当前支持 ASCII 标识符）。默认仅预览；执行前重读父任务/视图代际并验证内置作业名，单次发送公共 CANCEL TASK，目标为不会随普通同名视图重建而复用的 inner_mtmv_<MvId>，不在执行时重新解析可复用的视图名。该公共通用语句处理 MV 内置作业的行为已在 4.1.5 实测，不推断其他版本也支持。维护期间禁止伪造/复用这个生成名字。旧父代际已消失、错误/已终态任务或目标变化时拒绝继续；TaskId 基于进程时钟与随机值生成，非单调，也未验证跨重启永不复用；不能用大小或最新一行推断归属，不能把父代际保护推广成 TaskId 碰撞/复用时的原子保证。故障或重启后须重读完整身份，不盲重试旧 ID。[取消实现](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/job/manager/JobManager.java)。

ACK 丢失、取消进程或超时后，已提交的 CANCEL 仍可能完成；不自动重试。收到 ACK 后还要观察精确任务终态；历史被清理、视图删除或读取失败都会报告无法确认。SUCCESS 可以先于取消到达，FAILED/CANCELED 也不能证明未曾写入；已有分区提交不会回滚，服务端终态本身也不是全部后台执行已排空的屏障。

取消一个 MV 任务不会禁用 ON COMMIT/定时刷新。调度隔离需在独占维护中另行完成，例如 Doris 的 PAUSE MATERIALIZED VIEW JOB；其公共暂停接口按视图名定位，没有按 MvId 比较暂停的条件，通用 PAUSE JOB 又拒绝内置 MV 作业，CLI 不把它包装成代际安全的暂停。暂停后的 MV 仍接受显式 REFRESH；禁止旧 runner、管理员或其他入口刷新/恢复/改配调度，不承诺暂停跨重启持久生效。重连、重启或配置变化后重新检查。[MV 调度处理器](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVJobManager.java)。

范围仍有限：SHOW LOAD 不覆盖所有 Stream Load/COPY 及通用调度 INSERT，EXPORT、备份/恢复、索引构建等仍需另核对。公共列表可能因对象删除、历史清理或服务端不可见记录而不完整；CLI 拒绝可见错误及已观察记录的消失，但首次检查前已被删除/隐藏的孤立任务、尚未登记的已获准命令，不能凭空快照排除。所有检查按当前数据库执行，不自动推断账号对作业或锁的所有权，不是全局服务端写入隔离证明。

**不提供 COLUMN 的 CLI 取消。** [4.1.5 SchemaChangeHandler](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/alter/SchemaChangeHandler.java) 实际忽略 CANCEL 中的 JobId，按表选当前作业；真实测试证实传入不存在的 ID 仍取消当前作业。先读 ID 再取消无法避免误取消后继作业，因此 CLI 只等待它结束。ROLLUP 的 [处理器](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/alter/MaterializedViewHandler.java) 会按精确 ID 选择作业。事务终态与已提交/尚未可见的边界见 [SHOW TRANSACTION](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/transaction/SHOW-TRANSACTION/) 和 [导入事务](https://doris.apache.org/docs/4.x/key-features/load-transaction/)。未提交普通 SQL 事务应由原会话显式 COMMIT/ROLLBACK、断开后核验或等待服务端超时；已提交的数据不会因账号退役、锁恢复或作业取消而回滚。

<a name="doc-locks"></a>

## 恢复精确 owner 并修复历史

owner 必须来自新的 lock status，而不是旧日志。转换门预览同时要求 gate 与 main owner（或 unlocked）；执行需 maintenance 和 force。主锁 recover 无预览，--force 确认执行；本流程还统一提供 --maintenance，使用 --retired-identity 时 CLI 也要求它。主锁恢复始终重新检查限定范围内的事务和作业；--retired-identity 还验证账号缺失及旧会话。未知布局、错误/缺失/变化的 owner 或未终结工作都会阻止恢复。两个命令都不会自动修复 dirty/applied 历史。

```sh
cargo sqlx-doris admin lock status
cargo sqlx-doris admin lock recover-transition --owner '<transition-owner UUID>' --lock-owner '<main-lock UUID>'
# Use --lock-owner unlocked only when lock status shows no main owner.
cargo sqlx-doris admin lock recover-transition --owner '<transition-owner UUID>' \
  --lock-owner '<main-lock UUID>' --maintenance --force
cargo sqlx-doris admin lock status
cargo sqlx-doris admin lock recover --owner '<current main-owner UUID>' --maintenance --force
cargo sqlx-doris admin migrate repair --version 123 --action mark-applied --force
# Alternative after reconciliation: --action remove permits the next run to execute the file.
```

repair 只修改当前 dirty 版本的元数据，不执行业务 SQL。mark-applied 记录 up 源码校验值，remove 会使下次 run 执行该文件，必须先确认重放安全；失败的 down 迁移需按实际撤销效果核对，不能盲目标记 up 为已应用。`database drop --force` 删除 URL 中的整个数据库。

DROP 结果需区分：not_submitted 表示未发送 owner DROP；uncertain 表示没有成功回执，必须查看实际状态；已确认但核验失败时需保留后继并重新观察。即使未发 owner DROP，主锁恢复也可能已改变协调元数据。一旦开始删除 owner，连接在成功、失败、取消后都会被淘汰。已提交命令可在进程/future 取消后完成，不可盲目重发。

<a name="doc-limits"></a>

## 隔离与覆盖边界

UUID 分区名称保护后继锁分区免受迟到 DROP，但不能阻止旧业务 SQL。KILL QUERY 只停止一次查询；KILL CONNECTION 不阻止重连。数字 ID 属于单 FE，重启后可复用，因此 CLI 不自动终止持久化旧 ID，也不把 PROCESSLIST 中缺失当作接管证明。

身份退役仅验证于 shared-nothing Doris 4.1.5、单 master FE、内置认证授权。已准入事务仍可 COMMIT，已提交作业仍可完成。排空列表不是原子快照，覆盖有限；已移除/孤立工作、其他作业、恢复/重绑定及未登记命令仍无法确认，详见[兼容范围](COMPATIBILITY.zh-CN.md)。
