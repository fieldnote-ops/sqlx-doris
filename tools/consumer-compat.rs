//! Copied into independent applications by scripts/test-consumer.
use sqlx_doris::{Backend, PoolBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let setup_macro = std::env::args().any(|argument| argument == "--setup-macro");
    let cleanup_macro = std::env::args().any(|argument| argument == "--cleanup-macro");
    for (backend, key) in [
        (Backend::MySql, "MYSQL_TEST_URL"),
        (Backend::Doris, "DORIS_TEST_URL"),
    ] {
        let db = PoolBuilder::new(backend)
            .max_connections(1)
            .connect(&std::env::var(key)?)
            .await?;
        let table = if setup_macro || cleanup_macro {
            "_sqlx_doris_consumer_macro_prepare".to_owned()
        } else {
            format!("_consumer_prepare_{}", std::process::id())
        };
        if cleanup_macro {
            ::sqlx::raw_sql(::sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
                .execute(db.pool())
                .await?;
            db.close().await;
            continue;
        }
        let definition = if backend == Backend::Doris {
            "(id BIGINT NOT NULL,v BIGINT NOT NULL) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            "(id BIGINT NOT NULL,v BIGINT NOT NULL)"
        };
        ::sqlx::raw_sql(::sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {table} {definition}"
        )))
        .execute(db.pool())
        .await?;
        if setup_macro {
            db.close().await;
            continue;
        }
        let insert = format!("INSERT INTO {table} VALUES (1,3)");
        let mut connection = db.pool().acquire().await?;
        let first = ::sqlx::query(::sqlx::AssertSqlSafe(insert.as_str()))
            .execute(&mut *connection)
            .await;
        assert_eq!(first?.rows_affected(), 1);
        for persistent in [true, true, false] {
            assert_eq!(
                ::sqlx::query(::sqlx::AssertSqlSafe(insert.as_str()))
                    .persistent(persistent)
                    .execute(&mut *connection)
                    .await?
                    .rows_affected(),
                1
            );
        }
        assert_eq!(
            ::sqlx::query(::sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?)"
            )))
            .bind(2_i64)
            .bind(6_i64)
            .execute(&mut *connection)
            .await?
            .rows_affected(),
            1
        );
        let rows: Vec<(i64, i64)> = ::sqlx::query_as(::sqlx::AssertSqlSafe(format!(
            "SELECT id,v FROM {table} ORDER BY id"
        )))
        .fetch_all(&mut *connection)
        .await?;
        assert_eq!(rows, [(1, 3), (1, 3), (1, 3), (1, 3), (2, 6)]);
        connection.close().await?;
        println!(
            "{backend:?}: direct application SQLx query, cache, uncached and bind paths passed; exactly five stored rows"
        );
        // Native Transaction has the application's own SQLx MySql identity.
        let mut tx: ::sqlx::Transaction<'_, ::sqlx::MySql> = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(3_i64)
        .bind(9_i64)
        .execute(&mut *tx)
        .await?;
        ::sqlx::raw_sql(::sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (6,18)"
        )))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(4_i64)
        .bind(12_i64)
        .execute(&mut *tx)
        .await?;
        tx.rollback().await?;
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(5_i64)
        .bind(15_i64)
        .execute(&mut *tx)
        .await?;
        drop(tx);
        let count: i64 = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {table}"
        )))
        .fetch_one(db.pool())
        .await?;
        assert_eq!(count, 7);
        println!(
            "{backend:?}: native application Transaction bind/text mix and commit/rollback/drop passed"
        );
        ::sqlx::raw_sql(::sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
            .execute(db.pool())
            .await?;
        let binary_table = format!("{table}_binary_time");
        let columns = if backend == Backend::Doris {
            "(id BIGINT,b STRING,t VARCHAR(64)) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            "(id BIGINT PRIMARY KEY,b LONGBLOB,t TIME(6))"
        };
        ::sqlx::raw_sql(::sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {binary_table}{columns}"
        )))
        .execute(db.pool())
        .await?;
        let time = ::sqlx::mysql::types::MySqlTime::new(
            ::sqlx::mysql::types::MySqlTimeSign::Negative,
            0,
            0,
            0,
            1,
        )?;
        let bytes: Vec<u8> = (0..=255).collect();
        for action in ["commit", "rollback", "drop"] {
            let mut tx: ::sqlx::Transaction<'_, ::sqlx::MySql> = db.pool().begin().await?;
            ::sqlx::query(::sqlx::AssertSqlSafe(format!(
                "INSERT INTO {binary_table} VALUES(?,?,?)"
            )))
            .bind(match action {
                "commit" => 1_i64,
                "rollback" => 3_i64,
                _ => 4_i64,
            })
            .bind(&bytes)
            .bind(time)
            .execute(&mut *tx)
            .await?;
            match action {
                "commit" => tx.commit().await?,
                "rollback" => tx.rollback().await?,
                _ => drop(tx),
            }
        }
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "INSERT INTO {binary_table} VALUES(?,?,?)"
        )))
        .bind(2_i64)
        .bind(b"utf8\0'\\".as_slice())
        .bind(time)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let actual: Vec<(i64, Vec<u8>, ::sqlx::mysql::types::MySqlTime)> =
            ::sqlx::query_as(::sqlx::AssertSqlSafe(format!(
                "SELECT id,b,CAST(t AS TIME(6)) FROM {binary_table} ORDER BY id"
            )))
            .fetch_all(db.pool())
            .await?;
        assert_eq!(
            actual,
            [(1, bytes.clone(), time), (2, b"utf8\0'\\".to_vec(), time)]
        );
        let selected: (i64, Vec<u8>, ::sqlx::mysql::types::MySqlTime) =
            ::sqlx::query_as(::sqlx::AssertSqlSafe(format!(
                "SELECT id,b,CAST(t AS TIME(6)) FROM {binary_table} WHERE b=?"
            )))
            .bind(&bytes)
            .fetch_one(db.pool())
            .await?;
        assert_eq!(selected, (1, bytes.clone(), time));
        println!(
            "{backend:?}: application's native WHERE b=? and byte bind preserved raw bytes and signed TIME without manual HEX"
        );
        let included: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {binary_table} WHERE b IN(?,?) ORDER BY id"
        )))
        .bind(&bytes)
        .bind(None::<Vec<u8>>)
        .fetch_all(db.pool())
        .await?;
        assert_eq!(included, [1]);
        let unknown: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {binary_table} WHERE b NOT IN(?,NULL) ORDER BY id"
        )))
        .bind(&bytes)
        .fetch_all(db.pool())
        .await?;
        assert!(unknown.is_empty());
        let replacement = vec![255_u8, 128, 0, 39, 92];
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "UPDATE {binary_table} SET b=? WHERE b IN(?,NULL)"
        )))
        .bind(&replacement)
        .bind(&bytes)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let actual: Vec<u8> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT b FROM {binary_table} WHERE b IN(?)"
        )))
        .bind(&replacement)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(actual, replacement);
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "DELETE FROM {binary_table} WHERE b NOT IN(?)"
        )))
        .bind(&replacement)
        .execute(&mut *tx)
        .await?;
        tx.rollback().await?;
        let count: i64 = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {binary_table}"
        )))
        .fetch_one(db.pool())
        .await?;
        assert_eq!(count, 2);
        println!(
            "{backend:?}: application's native BLOB IN/NOT IN binds preserved NULLs and transaction outcomes"
        );
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "INSERT INTO {binary_table} VALUES(3,?,?)"
        )))
        .bind(None::<Vec<u8>>)
        .bind(time)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let different: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {binary_table} WHERE b<>? ORDER BY id"
        )))
        .bind(&replacement)
        .fetch_all(db.pool())
        .await?;
        assert_eq!(different, [2]);
        let unknown: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {binary_table} WHERE b!=?"
        )))
        .bind(None::<Vec<u8>>)
        .fetch_all(db.pool())
        .await?;
        assert!(unknown.is_empty());
        let only_null: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {binary_table} WHERE ?<=>b"
        )))
        .bind(None::<Vec<u8>>)
        .fetch_all(db.pool())
        .await?;
        assert_eq!(only_null, [3]);
        let compare_bytes = vec![255_u8, 254, 0, 128];
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "UPDATE {binary_table} SET b=? WHERE b<=>?"
        )))
        .bind(&compare_bytes)
        .bind(None::<Vec<u8>>)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let actual: Vec<u8> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT b FROM {binary_table} WHERE b<=>?"
        )))
        .bind(&compare_bytes)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(actual, compare_bytes);
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "DELETE FROM {binary_table} WHERE b!=?"
        )))
        .bind(&replacement)
        .execute(&mut *tx)
        .await?;
        tx.rollback().await?;
        let count: i64 = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {binary_table}"
        )))
        .fetch_one(db.pool())
        .await?;
        assert_eq!(count, 3);
        let mut tx = db.pool().begin().await?;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "DELETE FROM {binary_table} WHERE ?<=>b"
        )))
        .bind(&compare_bytes)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        let count: i64 = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {binary_table}"
        )))
        .fetch_one(db.pool())
        .await?;
        assert_eq!(count, 2);
        println!(
            "{backend:?}: application's native <>/!=/<=> byte binds preserved NULL truth and transaction outcomes"
        );
        let negative = ::sqlx::mysql::types::MySqlTime::new(
            ::sqlx::mysql::types::MySqlTimeSign::Negative,
            0,
            0,
            0,
            1,
        )?;
        for persistent in [true, true, false] {
            let got: ::sqlx::mysql::types::MySqlTime =
                ::sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                    .bind(negative)
                    .persistent(persistent)
                    .fetch_one(db.pool())
                    .await?;
            assert_eq!(got, negative);
        }
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "UPDATE {binary_table} SET t=? WHERE id=?"
        )))
        .bind(negative)
        .bind(1_i64)
        .execute(db.pool())
        .await?;
        let got: ::sqlx::mysql::types::MySqlTime = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(
            format!("SELECT t FROM {binary_table} WHERE id=?"),
        ))
        .bind(1_i64)
        .fetch_one(db.pool())
        .await?;
        assert_eq!(got, negative);
        let null: Option<::sqlx::mysql::types::MySqlTime> =
            ::sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                .bind(None::<::sqlx::mysql::types::MySqlTime>)
                .fetch_one(db.pool())
                .await?;
        assert!(null.is_none());
        println!(
            "{backend:?}: application's native outside-transaction TIME binds and original DTO preserved signed microseconds, NULL and cache"
        );
        let actual: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {binary_table} WHERE (b IN(?,NULL) OR t<=>?) AND NOT(t<>?) ORDER BY id"
        )))
        .bind(vec![222_u8])
        .bind(negative)
        .bind(negative)
        .fetch_all(db.pool())
        .await?;
        assert_eq!(actual, [1, 2]);
        println!(
            "{backend:?}: application's original mixed byte/TIME predicate and ordered binds preserved NULL truth"
        );
        let zero = ::sqlx::mysql::types::MySqlTime::ZERO;
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "UPDATE {binary_table} SET t=? WHERE id=1"
        )))
        .bind(zero)
        .execute(db.pool())
        .await?;
        for persistent in [true, true, false] {
            let ids: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {binary_table} WHERE t IN(?,?) ORDER BY id"
            )))
            .bind(negative)
            .bind("+00:00:00.000000")
            .persistent(persistent)
            .fetch_all(db.pool())
            .await?;
            assert_eq!(ids, [1, 2]);
            let ids: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {binary_table} WHERE t NOT IN(?,?,NULL) ORDER BY id"
            )))
            .bind(zero)
            .bind("00:00:00")
            .persistent(persistent)
            .fetch_all(db.pool())
            .await?;
            assert!(ids.is_empty());
        }
        println!(
            "{backend:?}: application's original TIME IN/NOT IN binds preserved zero, mixed strings, NULL and cache"
        );
        for persistent in [true, true, false] {
            let ids: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {binary_table} WHERE b>=? AND b<? ORDER BY id"
            )))
            .bind(Vec::<u8>::new())
            .bind(vec![255_u8, 255, 255])
            .persistent(persistent)
            .fetch_all(db.pool())
            .await?;
            assert_eq!(ids, [1, 2]);
            let ids: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {binary_table} WHERE b>? ORDER BY id"
            )))
            .bind(None::<Vec<u8>>)
            .persistent(persistent)
            .fetch_all(db.pool())
            .await?;
            assert!(ids.is_empty());
        }
        println!(
            "{backend:?}: application's original byte range binds preserved ordering, NULL and cache"
        );
        for persistent in [true, true, false] {
            let ids: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {binary_table} WHERE b BETWEEN ? AND ? ORDER BY id"
            )))
            .bind(Vec::<u8>::new())
            .bind(vec![255_u8, 255, 255])
            .persistent(persistent)
            .fetch_all(db.pool())
            .await?;
            assert_eq!(ids, [1, 2]);
            let ids: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {binary_table} WHERE b NOT BETWEEN ? AND ? ORDER BY id"
            )))
            .bind(None::<Vec<u8>>)
            .bind(Vec::<u8>::new())
            .persistent(persistent)
            .fetch_all(db.pool())
            .await?;
            assert_eq!(ids, [1, 2]);
        }
        println!(
            "{backend:?}: application's original BETWEEN/NOT BETWEEN preserved inclusive boundaries, NULL false-range truth and cache"
        );
        ::sqlx::raw_sql(::sqlx::AssertSqlSafe(format!("DROP TABLE {binary_table}")))
            .execute(db.pool())
            .await?;
        println!(
            "{backend:?}: application's native Vec<u8>/MySqlTime binds preserved binary bytes, signed microseconds and transaction outcomes"
        );
        let defaults_table = format!("{table}_defaults");
        let schema = if backend == Backend::Doris {
            "(id BIGINT NOT NULL DEFAULT '42',title VARCHAR(64) NULL DEFAULT 'fresh',n BIGINT NULL DEFAULT '7',opt BIGINT NULL DEFAULT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            "(id BIGINT NOT NULL PRIMARY KEY DEFAULT '42',title VARCHAR(64) NULL DEFAULT 'fresh',n BIGINT NULL DEFAULT '7',opt BIGINT NULL DEFAULT NULL)"
        };
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {defaults_table}{schema}"
        )))
        .execute(db.pool())
        .await?;
        for persistent in [true, true, false] {
            let mut tx: ::sqlx::Transaction<'_, ::sqlx::MySql> = db.pool().begin().await?;
            ::sqlx::query(::sqlx::AssertSqlSafe(format!(
                "INSERT INTO {defaults_table}(title,id,n) VALUE(?,?,DEFAULT),(NULL,?,NULL)"
            )))
            .bind("app'默认😀")
            .bind(1_i64)
            .bind(2_i64)
            .persistent(persistent)
            .execute(&mut *tx)
            .await?;
            ::sqlx::query(::sqlx::AssertSqlSafe(format!(
                "INSERT INTO {defaults_table} VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT)"
            )))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            let actual: Vec<(i64, Option<String>, Option<i64>, Option<i64>)> =
                ::sqlx::query_as(::sqlx::AssertSqlSafe(format!(
                    "SELECT id,title,n,opt FROM {defaults_table} ORDER BY id"
                )))
                .fetch_all(db.pool())
                .await?;
            assert_eq!(
                actual,
                vec![
                    (1, Some("app'默认😀".to_owned()), Some(7), None),
                    (2, None, None, None),
                    (42, Some("fresh".to_owned()), Some(7), None),
                ]
            );
            let page: Vec<i64> = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {defaults_table} WHERE (? IS NULL OR n>=?) ORDER BY id LIMIT ? OFFSET ?"
            )))
            .bind(Some(7_i64)).bind(Some(7_i64)).bind(1_i64).bind(1_i64)
            .persistent(persistent).fetch_all(db.pool()).await?;
            assert_eq!(page, [42]);
            let conflict = ::sqlx::query(::sqlx::AssertSqlSafe(format!(
                "INSERT INTO {defaults_table}(id,title,n) VALUES(?,?,?)"
            )))
            .bind(1_i64)
            .bind("native conflict")
            .bind(99_i64)
            .persistent(persistent)
            .execute(db.pool())
            .await;
            if backend == Backend::Doris {
                let result = conflict?;
                assert_eq!(result.rows_affected(), 1);
                assert_eq!(result.last_insert_id(), 0);
            } else {
                let error = conflict.unwrap_err();
                assert_eq!(
                    error
                        .as_database_error()
                        .unwrap()
                        .downcast_ref::<::sqlx::mysql::MySqlDatabaseError>()
                        .number(),
                    1062
                );
            }
            let value: i64 = ::sqlx::query_scalar(::sqlx::AssertSqlSafe(format!(
                "SELECT n FROM {defaults_table} WHERE id=?"
            )))
            .bind(1_i64)
            .fetch_one(db.pool())
            .await?;
            assert_eq!(value, if backend == Backend::Doris { 99 } else { 7 });
            ::sqlx::query(::sqlx::AssertSqlSafe(format!(
                "DELETE FROM {defaults_table}"
            )))
            .execute(db.pool())
            .await?;
        }
        ::sqlx::query(::sqlx::AssertSqlSafe(format!(
            "DROP TABLE {defaults_table}"
        )))
        .execute(db.pool())
        .await?;
        println!(
            "{backend:?}: application's original named-column VALUE batches, DEFAULT/NULL and all-DEFAULT transaction writes preserved native MySql and cache"
        );
        println!(
            "{backend:?}: application's original optional filters and bound LIMIT/OFFSET pagination preserved native MySql"
        );
        println!(
            "{backend:?}: application's same INSERT binds preserved native conflict semantics and receipts"
        );
        db.close().await;
    }
    Ok(())
}
