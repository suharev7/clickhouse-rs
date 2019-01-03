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
//! ### DNS
//!
//! ```url
//! schema://user:password@host[:port]/database?param1=value1&...&paramN=valueN
//! ```
//!
//! parameters:
//!
//! - `compression` - Whether or not use compression (defaults to `none`). Possible choices:
//!     * `none`
//!     * `lz4`
//!
//! - `connection_timeout` - Timeout for connection (defaults to `500 ms`)
//! - `keepalive` - TCP keep alive timeout in milliseconds.
//! - `nodelay` - Whether to enable `TCP_NODELAY` (defaults to `true`).
//!
//! - `pool_max` - Lower bound of opened connections for `Pool` (defaults to `10`).
//! - `pool_min` - Upper bound of opened connections for `Pool` (defaults to `20`).
//!
//! - `ping_before_query` - Ping server every time before execute any query. (defaults to `true`).
//! - `send_retries` - Count of retry to send request to server. (defaults to `3`).
//! - `retry_timeout` - Amount of time to wait before next retry. (defaults to `5 sec`).
//! - `ping_timeout` - Timeout for ping (defaults to `500 ms`).
//!
//! example:
//! ```url
//! tcp://user:password@host:9000/clicks?compression=lz4&ping_timeout=42ms
//! ```
//!
//! ### Example
//!
//! ```rust
//! extern crate clickhouse_rs;
//! extern crate futures;
//!
//! use futures::Future;
//! use clickhouse_rs::{Pool, types::Block};
//! # use std::env;
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
//!     # let database_url = env::var("DATABASE_URL").unwrap_or("tcp://localhost:9000?compression=lz4".into());
//!     let pool = Pool::new(database_url);
//!
//!     let done = pool
//!        .get_handle()
//!        .and_then(move |c| c.execute(ddl))
//!        .and_then(move |c| c.insert("payment", block))
//!        .and_then(move |c| c.query("SELECT * FROM payment").all())
//!        .and_then(move |(_, block)| {
//!            Ok(for row in block.rows() {
//!                let id: u32     = row.get("customer_id")?;
//!                let amount: u32 = row.get("amount")?;
//!                let name: &str  = row.get("account_name")?;
//!                println!("Found payment {}: {} {}", id, amount, name);
//!            })
//!        })
//!        .map_err(|err| eprintln!("database error: {}", err));
//!
//!     tokio::run(done)
//! }
//! ```

#![recursion_limit = "1024"]

extern crate byteorder;
extern crate chrono;
extern crate chrono_tz;
extern crate clickhouse_rs_cityhash_sys;
extern crate core;
extern crate failure;
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
extern crate url;

use std::fmt;

use futures::{Future, Stream};
use tokio::prelude::*;

use crate::{
    connecting_stream::ConnectingStream,
    errors::{DriverError, Error},
    io::{BoxFuture, ClickhouseTransport},
    pool::PoolBinding,
    retry_guard::RetryGuard,
    types::{Block, Cmd, Context, IntoOptions, Options, OptionsSource, Packet, Query, QueryResult},
};
pub use crate::pool::Pool;

mod binary;
mod client_info;
mod connecting_stream;
/// Error types.
pub mod errors;
mod io;
mod pool;
mod retry_guard;
/// Clickhouse types.
pub mod types;

macro_rules! try_opt {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Box::new(future::err(err)),
        }
    };
}

pub struct Client {
    _private: (),
}

/// Clickhouse client handle.
pub struct ClientHandle {
    inner: Option<ClickhouseTransport>,
    context: Context,
    pool: PoolBinding,
}

impl fmt::Debug for ClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ClientHandle")
            .field("server_info", &self.context.server_info)
            .finish()
    }
}

impl Client {
    #[deprecated(since = "0.1.4", note = "please use Pool to connect")]
    pub fn connect(options: Options) -> BoxFuture<ClientHandle> {
        Client::open(options.into_options_src())
    }

    pub(crate) fn open(source: OptionsSource) -> BoxFuture<ClientHandle> {
        let options = try_opt!(source.get()).as_ref().to_owned();
        let compress = options.compression;
        let timeout = options.connection_timeout;

        let context = Context {
            options: source.clone(),
            ..Context::default()
        };

        Box::new(
            ConnectingStream::new(&options.addr)
                .and_then(move |stream| {
                    stream.set_nodelay(options.nodelay)?;
                    stream.set_keepalive(options.keepalive)?;

                    let transport = ClickhouseTransport::new(stream, compress);
                    Ok(ClientHandle {
                        inner: Some(transport),
                        context,
                        pool: PoolBinding::None,
                    })
                })
                .map_err(|error| error.into())
                .and_then(|client| client.hello())
                .timeout(timeout)
                .map_err(Error::from),
        )
    }
}

impl ClientHandle {
    fn hello(mut self) -> BoxFuture<ClientHandle> {
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
                        future::ok::<_, Error>(Some(client))
                    }
                    Packet::Exception(e) => future::err::<_, Error>(Error::Server(e)),
                    _ => future::err::<_, Error>(Error::Driver(DriverError::UnexpectedPacket)),
                })
                .map(Option::unwrap),
        )
    }

    pub fn ping(mut self) -> BoxFuture<ClientHandle> {
        let context = self.context.clone();

        let options = try_opt!(context.options.get());

        let timeout = options.ping_timeout;
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
                        future::ok::<_, Error>(Some(client))
                    }
                    Packet::Exception(exception) => {
                        future::err::<_, Error>(Error::Server(exception))
                    }
                    _ => future::err::<_, Error>(Error::Driver(DriverError::UnexpectedPacket)),
                })
                .map(Option::unwrap)
                .timeout(timeout)
                .map_err(Error::from),
        )
    }

    /// Executes Clickhouse `query` on Conn.
    pub fn query<Q>(self, sql: Q) -> QueryResult
    where
        Query: From<Q>,
    {
        let query = Query::from(sql);
        QueryResult {
            client: self,
            query,
        }
    }

    /// Fetch data from table. It returns a block that contains all rows.
    #[deprecated(since = "0.1.7", note = "please use query(sql).all() instead")]
    pub fn query_all<Q>(self, sql: Q) -> BoxFuture<(ClientHandle, Block)>
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
                        future::ok::<_, Error>((h, bs))
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
                    Packet::Exception(exception) => {
                        future::err::<_, Error>(Error::Server(exception))
                    }
                    _ => future::err::<_, Error>(Error::Driver(DriverError::UnexpectedPacket)),
                })
                .map(|(client, blocks)| (client.unwrap(), Block::concat(&blocks[..])))
        })
    }

    /// Convenience method to prepare and execute a single SQL statement.
    pub fn execute<Q>(self, sql: Q) -> BoxFuture<ClientHandle>
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
                        future::ok::<_, Error>(Some(client))
                    }
                    Packet::Block(_) | Packet::ProfileInfo(_) | Packet::Progress(_) => {
                        future::ok::<_, Error>(acc)
                    }
                    Packet::Exception(exception) => {
                        future::err::<_, Error>(Error::Server(exception))
                    }
                    _ => future::err::<_, Error>(Error::Driver(DriverError::UnexpectedPacket)),
                })
                .map(Option::unwrap)
        })
    }

    /// Convenience method to insert block of data.
    pub fn insert<Q>(self, table: Q, block: Block) -> BoxFuture<ClientHandle>
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

    pub(crate) fn wrap_future<T, R, F>(self, f: F) -> BoxFuture<T>
    where
        F: FnOnce(ClientHandle) -> R + Send + 'static,
        R: Future<Item = T, Error = Error> + Send + 'static,
        T: Send + 'static,
    {
        let options = try_opt!(self.context.options.get());

        if options.ping_before_query {
            Box::new(self.check_connection().and_then(move |c| Box::new(f(c))))
        } else {
            Box::new(f(self))
        }
    }

    /// Check connection and try to reconnect if necessary.
    pub fn check_connection(mut self) -> BoxFuture<ClientHandle> {
        let pool: Option<Pool> = self.pool.clone().into();
        self.pool.detach();

        let source = self.context.options.clone();
        let options = try_opt!(source.get());
        let send_retries = options.send_retries;
        let retry_timeout = options.retry_timeout;

        let reconnect = move || -> BoxFuture<ClientHandle> {
            warn!("[reconnect]");
            match pool.clone() {
                None => Client::open(source.clone()),
                Some(p) => Box::new(p.get_handle()),
            }
        };

        Box::new(
            RetryGuard::new(self, |c| c.ping(), reconnect, send_retries, retry_timeout).and_then(
                |mut c| {
                    if !c.pool.is_attached() && c.pool.is_some() {
                        c.pool.attach();
                    }
                    Ok(c)
                },
            ),
        )
    }
}

#[cfg(test)]
mod test_misc {
    use std::env;

    lazy_static! {
        pub static ref DATABASE_URL: String =
            env::var("DATABASE_URL").unwrap_or("tcp://localhost:9000?compression=lz4".into());
    }
}
