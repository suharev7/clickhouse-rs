extern crate clickhouse_rs;
extern crate futures;

use std::env;

#[cfg(feature = "tls")]
fn main() {
    use futures::Future;
    use clickhouse_rs::Pool;

    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9440?secure=true&skip_verify=true".into()
    });
    let pool = Pool::new(database_url);

    let done = pool
        .get_handle()
        .and_then(move |c| c.query("SELECT 1 AS Value").fetch_all())
        .and_then(move |(_, block)| {
            println!("{:?}", block);
            Ok(())
        })
        .map_err(|err| eprintln!("database error: {}", err));

    tokio::run(done)
}

#[cfg(not(feature = "tls"))]
fn main() {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    panic!("Required ssl feature (cargo run --example tls --features tls)")
}