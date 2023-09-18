# Async ClickHouse Client 

[![Build Status](https://travis-ci.com/suharev7/clickhouse-rs.svg?branch=master)](https://travis-ci.com/suharev7/clickhouse-rs)
[![Crate info](https://img.shields.io/crates/v/clickhouse-rs.svg)](https://crates.io/crates/clickhouse-rs)
[![Documentation](https://docs.rs/clickhouse-rs/badge.svg)](https://docs.rs/clickhouse-rs)
[![dependency status](https://deps.rs/repo/github/suharev7/clickhouse-rs/status.svg)](https://deps.rs/repo/github/suharev7/clickhouse-rs)
[![Coverage Status](https://coveralls.io/repos/github/suharev7/clickhouse-rs/badge.svg)](https://coveralls.io/github/suharev7/clickhouse-rs)

Asynchronous [Yandex ClickHouse](https://clickhouse.yandex/) client library for rust programming language. 

## Installation
Library hosted on [crates.io](https://crates.io/crates/clickhouse-rs/).
```toml
[dependencies]
clickhouse-rs = "*"
```

## Supported data types

* Date
* DateTime
* Decimal(P, S)
* Float32, Float64
* String, FixedString(N)
* UInt8, UInt16, UInt32, UInt64, UInt128, Int8, Int16, Int32, Int64, Int128
* Nullable(T)
* Array(UInt/Int/Float/String/Date/DateTime)
* SimpleAggregateFunction(F, T)
* IPv4/IPv6
* UUID
* Bool

## DNS

```url
schema://user:password@host[:port]/database?param1=value1&...&paramN=valueN
```

parameters:

- `compression` - Whether or not use compression (defaults to `none`). Possible choices:
    * `none`
    * `lz4`

- `connection_timeout` - Timeout for connection (defaults to `500 ms`)
- `query_timeout` - Timeout for queries (defaults to `180 sec`).
- `insert_timeout` - Timeout for inserts (defaults to `180 sec`).
- `execute_timeout` - Timeout for execute (defaults to `180 sec`).
- `keepalive` - TCP keep alive timeout in milliseconds.
- `nodelay` - Whether to enable `TCP_NODELAY` (defaults to `true`).
 
- `pool_min` - Lower bound of opened connections for `Pool` (defaults to `10`).
- `pool_max` - Upper bound of opened connections for `Pool` (defaults to `20`).

- `ping_before_query` - Ping server every time before execute any query. (defaults to `true`).
- `send_retries` - Count of retry to send request to server. (defaults to `3`).
- `retry_timeout` - Amount of time to wait before next retry. (defaults to `5 sec`).
- `ping_timeout` - Timeout for ping (defaults to `500 ms`).


- `alt_hosts` - Comma separated list of single address host for load-balancing.

example:
```url
tcp://user:password@host:9000/clicks?compression=lz4&ping_timeout=42ms
```

## Optional features

`clickhouse-rs` puts some functionality behind optional features to optimize compile time 
for the most common use cases. The following features are available.

- `tokio_io` *(enabled by default)* — I/O based on [Tokio](https://tokio.rs/).
- `async_std` — I/O based on [async-std](https://async.rs/) (doesn't work together with `tokio_io`).
- `tls` — TLS support (allowed only with `tokio_io`).

## Example

```rust
use clickhouse_rs::{Block, Pool};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS payment (
            customer_id  UInt32,
            amount       UInt32,
            account_name Nullable(FixedString(3))
        ) Engine=Memory";

    let block = Block::new()
        .column("customer_id",  vec![1_u32,  3,  5,  7,  9])
        .column("amount",       vec![2_u32,  4,  6,  8, 10])
        .column("account_name", vec![Some("foo"), None, None, None, Some("bar")]);

    let pool = Pool::new(database_url);

    let mut client = pool.get_handle().await?;
    client.execute(ddl).await?;
    client.insert("payment", block).await?;
    let block = client.query("SELECT * FROM payment").fetch_all().await?;

    for row in block.rows() {
        let id: u32             = row.get("customer_id")?;
        let amount: u32         = row.get("amount")?;
        let name: Option<&str>  = row.get("account_name")?;
        println!("Found payment {}: {} {:?}", id, amount, name);
    }

    Ok(())
}
```
