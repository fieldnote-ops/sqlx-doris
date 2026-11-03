#![allow(dead_code)]

#[cfg(feature = "operations")]
use sqlx::Executor;
#[cfg(feature = "operations")]
use sqlx_doris::operations::TableName;
use sqlx_doris::{DorisPool, DorisPoolOptions};
use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

fn table_token() -> String {
    static NEXT_NAME: AtomicU64 = AtomicU64::new(0);
    format!(
        "_sdk_{}_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT_NAME.fetch_add(1, Ordering::Relaxed),
    )
}

pub fn qualified_table() -> String {
    format!("`sqlx_doris_test`.`{}`", table_token())
}

#[cfg(feature = "operations")]
pub fn name() -> TableName {
    TableName::new("sqlx_doris_test", table_token())
}

pub async fn client() -> DorisPool {
    DorisPoolOptions::new()
        .max_connections(2)
        .connect(&env::var("DORIS_TEST_URL").expect("DORIS_TEST_URL is required"))
        .await
        .unwrap()
}
#[cfg(feature = "operations")]
pub async fn drop_table(client: &DorisPool, name: &TableName) {
    client
        .execute(sqlx::AssertSqlSafe(format!("DROP TABLE {name}")))
        .await
        .unwrap();
}
