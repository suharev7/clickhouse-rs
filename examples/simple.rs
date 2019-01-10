extern crate clickhouse_rs;
extern crate futures;

use std::env;

use futures::Future;

use clickhouse_rs::{types::Block, Pool};

fn main() {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let ddl = "
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name String
        ) Engine=Memory";

    let block = Block::new()
        .add_column("customer_id", vec![1_u32, 3, 5, 7, 9])
        .add_column("amount", vec![2_u32, 4, 6, 8, 10])
        .add_column("account_name", vec!["foo", "", "", "", "bar"]);

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
                let name: &str = row.get("account_name")?;
                println!("Found payment {}: {} {}", id, amount, name);
            }
            Ok(())
        })
        .map_err(|err| eprintln!("database error: {}", err));

    tokio::run(done)
}
