extern crate clickhouse_rs;
extern crate futures;

use std::env;

use futures::Future;

use clickhouse_rs::{row, types::Block, Pool};

fn main() {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let ddl = "
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name Nullable(FixedString(3))
        ) Engine=Memory";

    let mut block = Block::new();
    block.push(row!{ customer_id: 1_u32, amount:  2_u32, account_name: Some("foo") }).unwrap();
    block.push(row!{ customer_id: 3_u32, amount:  4_u32, account_name: None::<&str> }).unwrap();
    block.push(row!{ customer_id: 5_u32, amount:  6_u32, account_name: None::<&str> }).unwrap();
    block.push(row!{ customer_id: 7_u32, amount:  8_u32, account_name: None::<&str> }).unwrap();
    block.push(row!{ customer_id: 9_u32, amount: 10_u32, account_name: Some("bar") }).unwrap();

    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    let pool = Pool::new(database_url);

    let done = pool
        .get_handle()
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("payment", block))
        .and_then(move |c| c.query("SELECT * FROM payment").fetch_all())
        .and_then(move |(_, block)| {
            for row in block.rows() {
                let id: u32 = row.get("customer_id")?;
                let amount: u32 = row.get("amount")?;
                let name: Option<&str> = row.get("account_name")?;
                println!("Found payment {}: {} {:?}", id, amount, name);
            }
            Ok(())
        })
        .map_err(|err| eprintln!("database error: {}", err));

    tokio::run(done)
}
