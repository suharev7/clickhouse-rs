extern crate clickhouse_rs;
extern crate futures;

use futures::Future;

use clickhouse_rs::{Block, Options, Pool};

pub fn main() {
    let ddl = "
        CREATE TABLE IF NOT EXISTS payment (
            customer_id UInt32,
            amount UInt32,
            account_name String
        ) Engine=Memory";

    let block = Block::new()
        .add_column("customer_id", vec![1_u32, 3, 5, 7, 9])
        .add_column("amount", vec![2_u32, 4, 6, 8, 10])
        .add_column("account_name", vec!["", "foo", "", "", "bar"]);

    let options = Options::new("127.0.0.1:9000".parse().unwrap()).with_compression();

    let pool = Pool::new(options);

    let done = pool
        .get_handle()
        .and_then(move |c| c.ping())
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("payment", block))
        .and_then(move |c| c.query_all("SELECT * FROM payment"))
        .and_then(move |(_, block)| {
            Ok(for row in 0..block.row_count() {
                let id: u32 = block.get(row, "customer_id")?;
                let amount: u32 = block.get(row, "amount")?;
                let name: &str = block.get(row, "account_name")?;
                println!("Found payment {}: {} {}", id, amount, name);
            })
        })
        .map_err(|err| eprintln!("database error: {}", err));

    tokio::run(done)
}
