use std::{env, error::Error};

use futures_util::StreamExt;

use clickhouse_rs::{Pool, types::Block};

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

    let mut client = pool.get_handle().await?;
    client.execute(ddl).await?;
    client.insert("payment", block).await?;
    let mut stream = client.query("SELECT * FROM payment").stream();

    while let Some(row) = stream.next().await {
        let row = row?;
        let id: u32 = row.get("customer_id")?;
        let amount: u32 = row.get("amount")?;
        let name: Option<&str> = row.get("account_name")?;
        println!("Found payment {}: {} {:?}", id, amount, name);
    }

    Ok(())
}
