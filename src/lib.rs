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
//! * Decimal(P, S)
//! * Float32, Float64
//! * String, FixedString(N)
//! * UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
//! * Nullable(T)
//! * Array(UInt/Int/String/Date/DateTime)
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
//! # use std::env;
//! use clickhouse_rs::{Block, Pool, errors::Error};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let ddl = r"
//!         CREATE TABLE IF NOT EXISTS payment (
//!             customer_id  UInt32,
//!             amount       UInt32,
//!             account_name Nullable(FixedString(3))
//!         ) Engine=Memory";
//!
//!     let block = Block::new()
//!         .column("customer_id",  vec![1_u32,  3,  5,  7,  9])
//!         .column("amount",       vec![2_u32,  4,  6,  8, 10])
//!         .column("account_name", vec![Some("foo"), None, None, None, Some("bar")]);
//!
//!     # let database_url = env::var("DATABASE_URL").unwrap_or("tcp://localhost:9000?compression=lz4".into());
//!     let pool = Pool::new(database_url);
//!
//!     let (_, block) = pool
//!         .get_handle().await?
//!         .execute(ddl).await?
//!         .insert("payment", block).await?
//!         .query("SELECT * FROM payment").fetch_all().await?;
//!
//!     for row in block.rows() {
//!         let id: u32     = row.get("customer_id")?;
//!         let amount: u32 = row.get("amount")?;
//!         let name: Option<&str>  = row.get("account_name")?;
//!         println!("Found payment {}: {} {:?}", id, amount, name);
//!     }
//!     Ok(())
//! }
//! ```


#![feature(type_ascription, async_closure)]
#![recursion_limit = "1024"]

use std::{fmt, future::Future, time::Duration};

use futures_core::{future::BoxFuture, Stream, stream::BoxStream};
use futures_util::{future, FutureExt, stream, StreamExt};
use log::{info, warn};
use tokio::timer::Timeout;

pub use crate::{
    pool::Pool,
    types::{block::Block, Options, SqlType},
};
use crate::{
    connecting_stream::ConnectingStream,
    errors::{DriverError, Error, Result},
    io::ClickhouseTransport,
    pool::PoolBinding,
    retry_guard::RetryGuard,
    types::{
        Cmd,
        Context,
        IntoOptions,
        OptionsSource,
        Packet,
        Query,
        QueryResult,
    },
};

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
            Err(err) => return Err(err),
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
    pub async fn connect(options: Options) -> Result<ClientHandle> {
        let source = options.into_options_src();
        Self::open(&source).await
    }

    pub(crate) async fn open(source: &OptionsSource) -> Result<ClientHandle> {
        let options = try_opt!(source.get());

        let compress = options.compression;
        let timeout = options.connection_timeout;

        let context = Context {
            options: source.clone(),
            ..Context::default()
        };

        with_timeout(async move {
            let stream = ConnectingStream::new(&options.addr).await?;
            stream.set_nodelay(options.nodelay)?;
            stream.set_keepalive(options.keepalive)?;

            let transport = ClickhouseTransport::new(stream, compress);
            let handle = ClientHandle {
                inner: Some(transport),
                context,
                pool: PoolBinding::None,
            };
            handle.hello().await
        }, timeout).await
    }
}

impl ClientHandle {
    pub(crate) async fn hello(mut self) -> Result<ClientHandle> {
        let context = self.context.clone();
        let pool = self.pool.clone();
        info!("[hello] -> {:?}", &context);

        let mut h = None;
        let mut stream = self
            .inner
            .take()
            .unwrap()
            .call(Cmd::Hello(context.clone()));

        while let Some(packet) = stream.next().await {
            match packet {
                Ok(Packet::Hello(inner, server_info)) => {
                    info!("[hello] <- {:?}", &server_info);
                    let context = Context {
                        server_info,
                        ..context.clone()
                    };
                    h = Some(ClientHandle {
                        inner: Some(inner),
                        context,
                        pool: pool.clone(),
                    });
                }
                Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                Err(e) => return Err(e.into()),
                _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
            }
        }

        Ok(h.unwrap())
    }

    pub async fn ping(mut self) -> Result<ClientHandle> {
        let context = self.context.clone();
        let timeout = try_opt!(self.context.options.get()).ping_timeout;

        let pool = self.pool.clone();

        with_timeout(async move {
            info!("[ping]");

            let mut h = None;
            let mut stream = self.inner.take().unwrap().call(Cmd::Ping);

            while let Some(packet) = stream.next().await {
                match packet {
                    Ok(Packet::Pong(inner)) => {
                        h = Some(ClientHandle {
                            inner: Some(inner),
                            context: context.clone(),
                            pool: pool.clone(),
                        });
                        info!("[pong]");
                    }
                    Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                    Err(e) => return Err(e.into()),
                    _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
                }
            }

            Ok(h.unwrap())
        }, timeout).await
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

    /// Convenience method to prepare and execute a single SQL statement.
    pub async fn execute<Q>(self, sql: Q) -> Result<ClientHandle>
    where
        Query: From<Q>,
    {
        let context = self.context.clone();
        let pool = self.pool.clone();
        let query = Query::from(sql);

        self.wrap_future(async move |mut c| {
            info!("[execute query] {}", query.get_sql());

            let mut h = None;
            let mut stream = c
                .inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()));

            while let Some(packet) = stream.next().await {
                match packet {
                    Ok(Packet::Eof(inner)) => {
                        h = Some(Self {
                            inner: Some(inner),
                            context: context.clone(),
                            pool: pool.clone(),
                        })
                    }
                    Ok(Packet::Block(_)) | Ok(Packet::ProfileInfo(_)) | Ok(Packet::Progress(_)) => (),
                    Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                    Err(e) => return Err(e.into()),
                    _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
                }
            }

            Ok(h.unwrap())
        }).await
    }

    /// Convenience method to insert block of data.
    pub async fn insert<Q>(self, table: Q, block: Block) -> Result<ClientHandle>
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

        self.wrap_future(async move |mut c| -> Result<ClientHandle> {
            info!("[insert]     {}", query.get_sql());
            let (mut c, b) = c.inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()))
                .read_block(context.clone(), pool.clone())
                .await?;

            let dst_block = b.unwrap();

            let casted_block = match block.cast_to(&dst_block) {
                Ok(value) => value,
                Err(err) => return Err(err),
            };

            let send_cmd = Cmd::Union(
                Box::new(Cmd::SendData(casted_block, context.clone())),
                Box::new(Cmd::SendData(Block::default(), context.clone())),
            );

            let (c, _) = c.inner
                .take()
                .unwrap()
                .call(send_cmd)
                .read_block(context, pool)
                .await?;

            Ok(c)
        }).await
    }

    pub(crate) async fn wrap_future<T, R, F>(self, f: F) -> Result<T>
    where
        F: FnOnce(Self) -> R + Send + 'static,
        R: Future<Output = Result<T>>,
        T: 'static,
    {
        let ping_before_query = try_opt!(self.context.options.get()).ping_before_query;

        if ping_before_query {
            let c = self.check_connection().await?;
            f(c).await
        } else {
            f(self).await
        }
    }

    pub(crate) fn wrap_stream<T, R, F>(self, f: F) -> BoxStream<'static, Result<T>>
    where
        F: FnOnce(Self) -> R + Send + 'static,
        R: Stream<Item = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let ping_before_query = match self.context.options.get() {
            Ok(val) => val.ping_before_query,
            Err(err) => return Box::pin(stream::once(future::err(err))),
        };

        if ping_before_query {
            let fut: BoxFuture<'static, BoxStream<'static, Result<T>>> =
                Box::pin(async move {
                    let inner: BoxStream<'static, Result<T>> =
                        match self.check_connection().await {
                            Ok(c) => Box::pin(f(c)),
                            Err(err) => Box::pin(stream::once(future::err(err))),
                        };
                    inner
                });

            Box::pin(fut.flatten_stream())
        } else {
            Box::pin(f(self))
        }
    }

    /// Check connection and try to reconnect if necessary.
    pub async fn check_connection(mut self) -> Result<ClientHandle> {
        let pool: Option<Pool> = self.pool.clone().into();
        self.pool.detach();

        let source = self.context.options.clone();

        let (send_retries, retry_timeout) = {
            let options = try_opt!(source.get());
            (options.send_retries, options.retry_timeout)
        };

        let reconnect = move || {
            let source = source.clone();
            let pool = pool.clone();
            async move {
                warn!("[reconnect]");
                match pool.clone() {
                    None => Client::open(&source).await,
                    Some(p) => Box::new(p.get_handle()).await,
                }
            }
        };

        let mut c = RetryGuard::new(self,
                                    |c| c.ping(),
                                    reconnect,
                                    send_retries,
                                    retry_timeout).await?;
        if !c.pool.is_attached() && c.pool.is_some() {
            c.pool.attach();
        }
        Ok(c)
    }
}

async fn with_timeout<F>(future: F, timeout: Duration) -> F::Output
where
    F: Future<Output = Result<ClientHandle>>
{
    match Timeout::new(future, timeout).await {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
pub(crate) mod test_misc {
    use std::env;

    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref DATABASE_URL: String = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    }
}
