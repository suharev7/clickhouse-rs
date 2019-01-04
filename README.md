# Tokio ClickHouse Client 

[![Build Status](https://travis-ci.com/suharev7/clickhouse-rs.svg?branch=master)](https://travis-ci.com/suharev7/clickhouse-rs)
[![Crate info](https://img.shields.io/crates/v/clickhouse-rs.svg)](https://crates.io/crates/clickhouse-rs)
[![Documentation](https://docs.rs/clickhouse-rs/badge.svg)](https://docs.rs/clickhouse-rs)
[![dependency status](https://deps.rs/repo/github/suharev7/clickhouse-rs/status.svg)](https://deps.rs/repo/github/suharev7/clickhouse-rs)

Tokio based asynchronous [Yandex ClickHouse](https://clickhouse.yandex/) client library for rust programming language. 

## Installation
Library hosted on [crates.io](https://crates.io/crates/clickhouse-rs/).
```toml
[dependencies]
clickhouse-rs = "*"
```

## Supported data types

* Date
* DateTime
* Float32, Float64
* String
* UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64

## DNS

```url
schema://user:password@host[:port]/database?param1=value1&...&paramN=valueN
```

parameters:

- `compression` - Whether or not use compression (defaults to `none`). Possible choices:
    * `none`
    * `lz4`

- `connection_timeout` - Timeout for connection (defaults to `500 ms`)
- `keepalive` - TCP keep alive timeout in milliseconds.
- `nodelay` - Whether to enable `TCP_NODELAY` (defaults to `true`).
 
- `pool_max` - Lower bound of opened connections for `Pool` (defaults to `10`).
- `pool_min` - Upper bound of opened connections for `Pool` (defaults to `20`).

- `ping_before_query` - Ping server every time before execute any query. (defaults to `true`).
- `send_retries` - Count of retry to send request to server. (defaults to `3`).
- `retry_timeout` - Amount of time to wait before next retry. (defaults to `5 sec`).
- `ping_timeout` - Timeout for ping (defaults to `500 ms`).

example:
```url
tcp://user:password@host:9000/clicks?compression=lz4&ping_timeout=42ms
```

## Example

```rust
extern crate clickhouse_rs;
extern crate futures;

use futures::Future;
use clickhouse_rs::{Pool, types::Block};

pub fn main() {
    let ddl = "
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name String
        ) Engine=Memory";

    let block = Block::new()
        .add_column("customer_id",  vec![1_u32,  3,  5,  7,     9])
        .add_column("amount",       vec![2_u32,  4,  6,  8,    10])
        .add_column("account_name", vec!["foo", "", "", "", "bar"]);

    let pool = Pool::new(database_url);
    
    let done = pool
       .get_handle()
       .and_then(move |c| c.execute(ddl))
       .and_then(move |c| c.insert("payment", block))
       .and_then(move |c| c.query("SELECT * FROM payment").all())
       .and_then(move |(_, block)| {
           Ok(for row in block.rows() {
               let id: u32     = row.get("customer_id")?;
               let amount: u32 = row.get("amount")?;
               let name: &str  = row.get("account_name")?;
               println!("Found payment {}: {} {}", id, amount, name);
           })
       })
       .map_err(|err| eprintln!("database error: {}", err));
    tokio::run(done)
}
```