extern crate clickhouse_rs;
extern crate futures;

use clickhouse_rs::{types::Block, Pool};
use futures::Future;
use std::env;

fn main() {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000".into());
    let pool = Pool::new(database_url);

    let ddl = "
        CREATE TABLE array_table (
            nums Array(UInt32),
            text Array(String)
        ) Engine=Memory";

    let query = "SELECT nums, text FROM array_table";

    let block = Block::new()
        .add_column("nums", vec![vec![1_u32, 2, 3], vec![4, 5, 6]])
        .add_column("text", vec![vec!["A", "B", "C"], vec!["D", "E"]]);

    let done = pool
        .get_handle()
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS array_table"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("array_table", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block): (_, Block)| {
            for row in block.rows() {
                let nums: Vec<u32> = row.get("nums")?;
                let text: Vec<&str> = row.get("text")?;
                println!("{:?},\t{:?}", nums, text);
            }
            Ok(())
        })
        .map_err(|err| eprintln!("database error: {}", err));

    tokio::run(done)
}
