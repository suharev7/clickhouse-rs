//! ## clickhouse-rs
//! Tokio based asynchronous [Yandex ClickHouse](https://clickhouse.yandex/) client library for rust programming language.
//!
//! ### Installation
//! Library hosted on [crates.io](https://crates.io/crates/clickhouse-rs/).
//!
//! ```toml
//! [dependencies]
//! clickhouse-rs = "*"
//! ```
//!
//! ### Supported data types
//!
//! * Date
//! * DateTime
//! * Float32, Float64
//! * String
//! * UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
//!
//! ### Example
//!
//! ```rust
//! extern crate clickhouse_rs;
//! extern crate futures;
//!
//! use clickhouse_rs::{Block, Pool, Options};
//! use futures::Future;
//!
//! pub fn main() {
//!     let ddl = "
//!         CREATE TABLE IF NOT EXISTS payment (
//!             customer_id  UInt32,
//!             amount       UInt32,
//!             account_name String
//!         ) Engine=Memory";
//!
//!     let block = Block::new()
//!         .add_column("customer_id",  vec![1_u32,  3,  5,  7,     9])
//!         .add_column("amount",       vec![2_u32,  4,  6,  8,    10])
//!         .add_column("account_name", vec!["foo", "", "", "", "bar"]);
//!
//!     let options = Options::new("127.0.0.1:9000".parse().unwrap())
//!         .with_compression();
//!
//!     let pool = Pool::new(options);
//!
//!     let done = pool
//!         .get_handle()
//!         .and_then(move |c| c.execute(ddl))
//!         .and_then(move |c| c.insert("payment", block))
//!         .and_then(move |c| c.query_all("SELECT * FROM payment"))
//!         .and_then(move |(_, block)| {
//!             Ok(for row in block.rows() {
//!                 let id: u32     = row.get("customer_id")?;
//!                 let amount: u32 = row.get("amount")?;
//!                 let name: &str  = row.get("account_name")?;
//!                 println!("Found payment {}: {} {}", id, amount, name);
//!             })
//!         }).map_err(|err| eprintln!("database error: {}", err));
//!
//!     tokio::run(done)
//! }
//! ```

extern crate byteorder;
extern crate chrono;
extern crate chrono_tz;
extern crate clickhouse_rs_cityhash_sys;
extern crate core;
#[macro_use]
extern crate futures;
extern crate hostname;
#[cfg(test)]
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate lz4;
#[cfg(test)]
extern crate rand;
extern crate tokio;

use std::fmt;
use std::io::Error;

use futures::{Future, Stream};
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio_timer::timeout::Error as TimeoutError;

pub use crate::block::Block;
use crate::block::BlockEx;
use crate::io::{ClickhouseTransport, IoFuture};
pub use crate::pool::Pool;
use crate::retry_guard::RetryGuard;
use crate::types::query::QueryEx;
pub use crate::types::{ClickhouseError, Options, SqlType};
use crate::types::{ClickhouseResult, Cmd, Context, Packet, Query};

mod binary;
mod block;
mod client_info;
mod column;
mod io;
mod pool;
mod retry_guard;
mod types;

pub struct Client {
    _private: (),
}

/// Clickhouse client handle.
pub struct ClientHandle {
    inner: Option<ClickhouseTransport>,
    context: Context,
    pool: Option<Pool>,
}

impl fmt::Debug for ClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("ClientHandle")
            .field("server_info", &self.context.server_info)
            .finish()
    }
}

impl Client {
    #[deprecated(since = "0.1.4", note = "please use Pool to connect")]
    pub fn connect(options: Options) -> IoFuture<ClientHandle> {
        Client::open(options)
    }

    fn open(options: Options) -> IoFuture<ClientHandle> {
        let compress = options.compression;

        let context = Context {
            options: options.clone(),
            ..Context::default()
        };

        Box::new(
            TcpStream::connect(&options.addr)
                .and_then(move |stream| {
                    stream.set_nodelay(options.nodelay)?;
                    stream.set_keepalive(options.keepalive)?;

                    let transport = ClickhouseTransport::new(stream, compress);
                    Ok(ClientHandle {
                        inner: Some(transport),
                        context,
                        pool: None,
                    })
                })
                .and_then(|client| client.hello()),
        )
    }
}

impl ClientHandle {
    fn hello(mut self) -> IoFuture<ClientHandle> {
        let context = self.context.clone();
        let pool = self.pool.clone();
        info!("[hello] -> {:?}", &context);
        Box::new(
            self.inner
                .take()
                .unwrap()
                .call(Cmd::Hello(context.clone()))
                .fold(None, move |_, packet| match packet {
                    Packet::Hello(inner, server_info) => {
                        info!("[hello] <- {:?}", &server_info);
                        let context = Context {
                            server_info,
                            ..context.clone()
                        };
                        let client = ClientHandle {
                            inner: Some(inner),
                            context,
                            pool: pool.clone(),
                        };
                        future::ok::<_, std::io::Error>(Some(client))
                    }
                    Packet::Exception(e) => future::err(ClickhouseError::Internal(e).into()),
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(Option::unwrap),
        )
    }

    pub fn ping(mut self) -> IoFuture<ClientHandle> {
        let context = self.context.clone();
        let timeout = context.options.ping_timeout;
        let pool = self.pool.clone();
        info!("[ping]");
        Box::new(
            self.inner
                .take()
                .unwrap()
                .call(Cmd::Ping)
                .fold(None, move |_, packet| match packet {
                    Packet::Pong(inner) => {
                        let client = ClientHandle {
                            inner: Some(inner),
                            context: context.clone(),
                            pool: pool.clone(),
                        };
                        info!("[pong]");
                        future::ok::<_, std::io::Error>(Some(client))
                    }
                    Packet::Exception(e) => future::err(ClickhouseError::Internal(e).into()),
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(Option::unwrap)
                .timeout(timeout)
                .map_err(into_io_error),
        )
    }

    /// Fetch data from table. It returns a block that contains all rows.
    pub fn query_all<Q>(self, sql: Q) -> IoFuture<(ClientHandle, Block)>
    where
        Query: From<Q>,
    {
        let context = self.context.clone();
        let pool = self.pool.clone();
        let query = Query::from(sql);
        let init = (None, vec![]);

        self.wrap_future(move |mut c| {
            info!("[send query] {}", query.get_sql());
            c.inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()))
                .fold(init, move |(h, mut bs), packet| match packet {
                    Packet::Block(b) => {
                        if !b.is_empty() {
                            bs.push(b);
                        }
                        future::ok::<_, std::io::Error>((h, bs))
                    }
                    Packet::Eof(inner) => {
                        let client = ClientHandle {
                            inner: Some(inner),
                            context: context.clone(),
                            pool: pool.clone(),
                        };
                        future::ok((Some(client), bs))
                    }
                    Packet::ProfileInfo(_) | Packet::Progress(_) => future::ok((h, bs)),
                    Packet::Exception(e) => future::err(ClickhouseError::Internal(e).into()),
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(|(client, blocks)| (client.unwrap(), Block::concat(&blocks[..])))
        })
    }

    /// Convenience method to prepare and execute a single SQL statement.
    pub fn execute<Q>(self, sql: Q) -> IoFuture<ClientHandle>
    where
        Query: From<Q>,
    {
        let context = self.context.clone();
        let pool = self.pool.clone();
        let query = Query::from(sql);
        self.wrap_future(|mut c| {
            info!("[execute]    {}", query.get_sql());
            c.inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()))
                .fold(None, move |acc, packet| match packet {
                    Packet::Eof(inner) => {
                        let client = ClientHandle {
                            inner: Some(inner),
                            context: context.clone(),
                            pool: pool.clone(),
                        };
                        future::ok::<_, std::io::Error>(Some(client))
                    }
                    Packet::Block(_) | Packet::ProfileInfo(_) | Packet::Progress(_) => {
                        future::ok::<_, std::io::Error>(acc)
                    }
                    Packet::Exception(exception) => {
                        future::err(ClickhouseError::Internal(exception).into())
                    }
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(Option::unwrap)
        })
    }

    /// Convenience method to insert block of data.
    pub fn insert<Q>(self, table: Q, block: Block) -> IoFuture<ClientHandle>
    where
        Query: From<Q>,
    {
        let names: Vec<_> = block
            .as_ref()
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect();
        let fields = names.join(", ");

        let query = Query::from(table)
            .map_sql(|table| format!("INSERT INTO {} ({}) VALUES", table, fields));

        let context = self.context.clone();
        let pool = self.pool.clone();

        let send_cmd = Cmd::Union(
            Box::new(Cmd::SendData(block, context.clone())),
            Box::new(Cmd::SendData(Block::default(), context.clone())),
        );

        self.wrap_future(|mut c| {
            info!("[insert]     {}", query.get_sql());
            c.inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()))
                .read_block(context.clone(), pool.clone())
                .and_then(move |mut c| {
                    c.inner
                        .take()
                        .unwrap()
                        .call(send_cmd)
                        .read_block(context, pool)
                })
        })
    }

    fn wrap_future<T, R, F>(self, f: F) -> IoFuture<T>
    where
        F: FnOnce(ClientHandle) -> R + Send + 'static,
        R: Future<Item = T, Error = Error> + Send + 'static,
        T: 'static,
    {
        if self.context.options.ping_before_query {
            Box::new(self.check_connection().and_then(move |c| Box::new(f(c))))
        } else {
            Box::new(f(self))
        }
    }

    /// Check connection and try to reconnect is necessary.
    pub fn check_connection(mut self) -> IoFuture<ClientHandle> {
        let pool = self.pool.take();
        let saved_pool = pool.clone();
        let options = self.context.options.clone();
        let send_retries = options.send_retries;
        let retry_timeout = options.retry_timeout;

        let reconnect = move || -> IoFuture<ClientHandle> {
            warn!("[reconnect]");
            match pool.clone() {
                None => Client::open(options.clone()),
                Some(p) => Box::new(p.get_handle()),
            }
        };

        Box::new(
            RetryGuard::new(self, |c| c.ping(), reconnect, send_retries, retry_timeout).and_then(
                |mut c| {
                    c.pool = saved_pool;
                    Ok(c)
                },
            ),
        )
    }
}

fn into_io_error(error: TimeoutError<Error>) -> Error {
    match error.into_inner() {
        None => ClickhouseError::Timeout.into(),
        Some(inner) => inner,
    }
}
