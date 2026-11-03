<a name="doc-top"></a>

# SQLx 迁移

[English](MIGRATIONS.md) | [简体中文](MIGRATIONS.zh-CN.md)

[文档首页](README.zh-CN.md)

迁移使用官方 SQLx 与原生 Doris 驱动。执行中断时先按[异常恢复](RECOVERY.zh-CN.md)核对，不要直接重跑不确定的文件。

<a name="doc-contents"></a>

## 目录

- [配置 Rust API](#doc-api)
- [通过 CLI 执行迁移](#doc-commands)
- [DDL、dirty 历史与完成](#doc-execution)
- [锁协议与失败边界](#doc-locking)

<a name="doc-api"></a>

## 配置 Rust API

嵌入文件时，将应用依赖条目更新为官方 `macros` + `migrate` 和驱动 `migrate`；仅开启官方 `derive` 不提供 migrate!。不需要开启驱动查询宏。

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["macros", "migrate"] }
sqlx-doris = { path = "../sqlx-doris-clean/crates/sqlx-doris", features = ["migrate"] }
```

将下列函数加入应用；migrations 目录相对于应用 Cargo manifest：

```rust
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn migrate(pool: &sqlx_doris::DorisPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
```

运行时读取目录使用 `Migrator::new(Path::new("./migrations")).await?`。文件名为 `<version>_<description>.sql`，可逆迁移使用 `.up.sql`/`.down.sql` 配对。嵌入文件变化后需触发重建，可在 build.rs 输出 `cargo:rerun-if-changed=migrations`。

<a name="doc-commands"></a>

## 通过 CLI 执行迁移

```sh
export DATABASE_URL='doris://administrator:password@doris-fe:9030/app'
cargo sqlx-doris database create
cargo sqlx-doris migrate add initial --reversible
cargo sqlx-doris migrate run
cargo sqlx-doris migrate info
cargo sqlx-doris migrate revert
cargo sqlx-doris migrate revert --target 0
```

先创建目标库，并在 run 前编辑生成的 SQL。无 target 的 revert 只撤销一对已应用可逆文件；target 0 明确撤销至版本 0。在叶子命令前使用 `migrate --source <directory>` 和 `--table`；`run --target <version>` 应用到指定版本。单 BE 夹具需指定全局 `--replication-num 1`，元数据默认 3 副本。`--ddl-timeout` 默认 300 秒、`--lock-timeout` 默认 30 秒。info 只读取历史，不创建状态表，并指出缺失或变更的源码。

<a name="doc-execution"></a>

## DDL、dirty 历史与完成

Doris 不提供整份迁移的原子 DDL。驱动在业务 SQL 前持久化 dirty 状态，验证版本和 SHA-384 校验值，并等待异步 COLUMN/ROLLUP 完成后再执行下一步。迁移期间暂时关闭 Group Commit，成功后恢复。成功 up 写入 applied，成功 down 删除对应记录；已完成语句不会自动回滚或重放。

脚本支持表、普通视图、索引 DDL 与普通 DML/查询；拒绝事务控制、SET、异步导入、CREATE MATERIALIZED VIEW、数据库/Catalog 切换及内部迁移表引用。异步 ALTER 必须操作当前库；MV 和导入使用专门 API。

<a name="doc-locking"></a>

## 锁协议与失败边界

同一数据库的 runner 共享持久 `_sqlx_migrations_lock` 与 `_sqlx_migrations_gate` 表。每表保留 [-1,0) 的 sqlx_anchor；新的 sqlx_owner_<UUID> 分区以原子 ADD PARTITION 争用 [0,1) 范围。释放/恢复只删除该 UUID 分区，不删除父表或 anchor；禁止复用 owner 名称或修改内部布局。中断的门不自动接管，未知或早期注释表布局直接拒绝，不自动转换。

正常完成或健康关闭会释放锁。若 ping/flush 清除了待清理健康锁，该连接报错并被淘汰。崩溃、取消、部分或不确定执行保留持久锁与 dirty/applied 历史。主锁释放/恢复通过转换门串行校验 owner 与删除；主锁 DROP 不确定时保留门并淘汰连接，需用新连接查看实际 owner 后再显式恢复。

owner 检查位于 dirty 标记、SQL、异步轮询及完成标记之前；它不能原子阻止检查后已排队的业务 SQL，普通查询也不在这些检查点内。必须停止、隔离旧执行者并核对已准入工作。只有外部串行部署才能使用 `Migrator::set_locking(false)` 或 `migrate --no-lock`。[异常恢复流程](RECOVERY.zh-CN.md)区分预览、确认和不确定结果。
