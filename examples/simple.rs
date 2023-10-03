use clickhouse_rs::{row, types::Block, Pool};
use futures_util::StreamExt;
use std::{env, error::Error};

async fn execute(database_url: String) -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let ddl = r"
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name Nullable(FixedString(3))
        ) Engine=Memory";

    let mut block = Block::with_capacity(5);
    block.push(row! { customer_id: 1_u32, amount:  2_u32, account_name: Some("foo") })?;
    block.push(row! { customer_id: 3_u32, amount:  4_u32, account_name: None::<&str> })?;
    block.push(row! { customer_id: 5_u32, amount:  6_u32, account_name: None::<&str> })?;
    block.push(row! { customer_id: 7_u32, amount:  8_u32, account_name: None::<&str> })?;
    block.push(row! { customer_id: 9_u32, amount: 10_u32, account_name: Some("bar") })?;

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
        println!("Found payment {id}: {amount} {name:?}");
    }

    Ok(())
}

#[cfg(all(feature = "tokio_io", not(feature = "tls")))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    execute(database_url).await
}

#[cfg(all(feature = "tokio_io", feature = "tls"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "tcp://localhost:9440?secure=true&skip_verify=true".into());
    execute(database_url).await
}

#[cfg(feature = "async_std")]
fn main() {
    use async_std::task;
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    task::block_on(execute(database_url)).unwrap();
}
