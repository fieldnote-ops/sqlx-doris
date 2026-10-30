//! Native logical types need explicit SQLx macro output type hints.
use sqlx_doris::{
    Backend, PoolBuilder, sqlx,
    types::{Array, BigDecimal, LargeInt},
};
use std::str::FromStr;

#[derive(Debug, PartialEq)]
struct NativeRow {
    wide: LargeInt,
    amount: BigDecimal,
    items: Array<Option<LargeInt>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = PoolBuilder::new(Backend::Doris)
        .connect(&std::env::var("DORIS_URL")?)
        .await?;
    let wide = LargeInt(i128::MAX);
    let amount = BigDecimal::from_str(
        "9999999999999999999999999999999999999999999999.123456789012345678901234567890",
    )?;
    let items = Array(vec![Some(LargeInt(i128::MIN)), None, Some(wide)]);
    let actual = sqlx::query_as!(
        NativeRow,
        "SELECT CAST(? AS LARGEINT) AS `wide!: LargeInt`,CAST(? AS DECIMAL(76,30)) \
            AS `amount!: BigDecimal`,TO_JSON(CAST(? AS ARRAY<LARGEINT>)) AS `items!: \
            Array<Option<LargeInt>>`",
        wide,
        amount,
        items
    )
    .fetch_one(db.pool())
    .await?;
    assert_eq!(
        actual,
        NativeRow {
            wide,
            amount,
            items
        }
    );
    db.close().await;
    println!("Doris macro explicit native types preserve precision and NULLs");
    Ok(())
}
