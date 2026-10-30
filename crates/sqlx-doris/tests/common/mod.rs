#![allow(dead_code)]

use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{self, Executor},
};
use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub fn name() -> TableName {
    static NEXT_NAME: AtomicU64 = AtomicU64::new(0);
    TableName::new(
        "sqlx_doris_test",
        format!(
            "_sdk_{}_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_NAME.fetch_add(1, Ordering::Relaxed),
        ),
    )
}

pub fn doris_4_0_8() -> bool {
    env::var("DORIS_TEST_VERSION").is_ok_and(|version| version == "4.0.8")
}
pub async fn client(backend: Backend) -> Client {
    PoolBuilder::new(backend)
        .max_connections(2)
        .connect(
            &env::var(match backend {
                Backend::MySql => "MYSQL_TEST_URL",
                Backend::Doris => "DORIS_TEST_URL",
            })
            .expect("dedicated test URL required"),
        )
        .await
        .unwrap()
}
pub async fn drop_table(client: &Client, name: &TableName) {
    client
        .pool()
        .execute(sqlx::AssertSqlSafe(format!("DROP TABLE {name}")))
        .await
        .unwrap();
}
