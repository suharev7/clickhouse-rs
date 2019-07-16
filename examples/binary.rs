extern crate clickhouse_rs;
extern crate futures;

use std::env;

use clickhouse_rs::{types::Block, Pool};
use futures::Future;

fn main() {
    let ddl = "
        CREATE TABLE IF NOT EXISTS test_blob (
            text        String,
            fx_text     FixedString(4),
            opt_text    Nullable(String),
            fx_opt_text Nullable(FixedString(4))
        ) Engine=Memory";

    let block = Block::new()
        .add_column("text", vec![[0, 159, 146, 150].as_ref(), b"ABCD"])
        .add_column("fx_text", vec![b"ABCD".as_ref(), &[0, 159, 146, 150]])
        .add_column("opt_text", vec![Some(vec![0, 159, 146, 150]), None])
        .add_column("fx_opt_text", vec![None, Some(vec![0, 159, 146, 150])]);

    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    let pool = Pool::new(database_url);

    let done = pool
        .get_handle()
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("test_blob", block))
        .and_then(move |c| {
            c.query("SELECT text, fx_text, opt_text, fx_opt_text FROM test_blob")
                .fetch_all()
        })
        .and_then(move |(_, block)| {
            for row in block.rows() {
                let text: &[u8] = row.get("text")?;
                let fx_text: &[u8] = row.get("fx_text")?;
                let opt_text: Option<&[u8]> = row.get("opt_text")?;
                let fx_opt_text: Option<&[u8]> = row.get("fx_opt_text")?;
                println!(
                    "{:?}\t{:?}\t{:?}\t{:?}",
                    text, fx_text, opt_text, fx_opt_text
                );
            }
            Ok(())
        })
        .map_err(|err| eprintln!("database error: {}", err));

    tokio::run(done)
}
