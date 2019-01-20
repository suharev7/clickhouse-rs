extern crate clickhouse_rs;
extern crate futures;

use std::env;

use futures::Future;

use clickhouse_rs::Pool;
use std::time::Instant;

fn foo(n: u32) {
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    let pool = Pool::new(database_url);

    let start = Instant::now();

    let sql = format!("
        SELECT
        toString(number), toString(number), number, number, toString(number), toString(number), number, number, toString(number), toString(number)
        FROM system.numbers LIMIT {}
    ", n);

    let done = pool
        .get_handle()
        .and_then(move |c| c.query(sql).fetch_all())
        .and_then(move |(_, _block)| {
            let spent = start.elapsed();
            println!("({}, {:?}),", n, spent.as_millis());
            Ok(())
        })
        .map_err(|err| eprintln!("database error: {}", err));

    tokio::run(done)
}

pub fn main() {
    // env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

//    println!("START");
//    foo(14000);

    let mut n = 0_u32;
    while n < 1_500_001 {
        foo(n);
        n += 100_000;
    }
}
