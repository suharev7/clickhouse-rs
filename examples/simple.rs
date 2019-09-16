use std::env;

use clickhouse_rs::{types::Block, Pool};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let ddl = r"
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name Nullable(FixedString(3))
        ) Engine=Memory";

    let block = Block::new()
        .column("customer_id", vec![1_u32, 3, 5, 7, 9])
        .column("amount", vec![2_u32, 4, 6, 8, 10])
        .column(
            "account_name",
            vec![Some("foo"), None, None, None, Some("bar")],
        );

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    let pool = Pool::new(database_url);

    let (_, block) = pool
        .get_handle().await?
        .execute(ddl).await?
        .insert("payment", block).await?
        .query("SELECT * FROM payment").fetch_all().await?;

    for row in block.rows() {
        let id: u32 = row.get("customer_id")?;
        let amount: u32 = row.get("amount")?;
        let name: Option<&str> = row.get("account_name")?;
        println!("Found payment {}: {} {:?}", id, amount, name);
    }
    Ok(())
}
