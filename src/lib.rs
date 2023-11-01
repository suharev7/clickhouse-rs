//! ## clickhouse-rs
//! Asynchronous [Yandex ClickHouse](https://clickhouse.yandex/) client library for rust programming language.
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
//! * SimpleAggregateFunction(F, T)
//! * IPv4/IPv6
//! * UUID
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
//! - `readonly` - Restricts permissions for read data, write data and change settings queries. (defaults to `none`). Possible choices:
//!     * `0` - All queries are allowed.
//!     * `1` - Only read data queries are allowed.
//!     * `2` - Read data and change settings queries are allowed.
//!
//! - `connection_timeout` - Timeout for connection (defaults to `500 ms`)
//! - `keepalive` - TCP keep alive timeout in milliseconds.
//! - `nodelay` - Whether to enable `TCP_NODELAY` (defaults to `true`).
//!
//! - `pool_min` - Lower bound of opened connections for `Pool` (defaults to `10`).
//! - `pool_max` - Upper bound of opened connections for `Pool` (defaults to `20`).
//!
//! - `ping_before_query` - Ping server every time before execute any query. (defaults to `true`).
//! - `send_retries` - Count of retry to send request to server. (defaults to `3`).
//! - `retry_timeout` - Amount of time to wait before next retry. (defaults to `5 sec`).
//! - `ping_timeout` - Timeout for ping (defaults to `500 ms`).
//!
//! - `alt_hosts` - Comma separated list of single address host for load-balancing.
//!
//! example:
//! ```url
//! tcp://user:password@host:9000/clicks?compression=lz4&ping_timeout=42ms
//! ```
//!
//! ## Optional features
//!
//! `clickhouse-rs` puts some functionality behind optional features to optimize compile time
//! for the most common use cases. The following features are available.
//!
//! - `tokio_io` *(enabled by default)* — I/O based on [Tokio](https://tokio.rs/).
//! - `async_std` — I/O based on [async-std](https://async.rs/) (doesn't work together with `tokio_io`).
//! - `tls` — TLS support (allowed only with `tokio_io`).
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
//!     let mut client = pool.get_handle().await?;
//!     client.execute(ddl).await?;
//!     client.insert("payment", block).await?;
//!     let block = client.query("SELECT * FROM payment").fetch_all().await?;
//!
//!     for row in block.rows() {
//!         let id: u32             = row.get("customer_id")?;
//!         let amount: u32         = row.get("amount")?;
//!         let name: Option<&str>  = row.get("account_name")?;
//!         println!("Found payment {id}: {amount} {name:?}");
//!     }
//!     Ok(())
//! }
//! ```

#![recursion_limit = "1024"]

use std::{fmt, future::Future, time::Duration};

use futures_util::{
    future, future::BoxFuture, future::FutureExt, stream, stream::BoxStream, StreamExt,
};
use log::{info, warn};

use crate::{
    connecting_stream::ConnectingStream,
    errors::{DriverError, Error, Result},
    io::ClickhouseTransport,
    pool::PoolBinding,
    retry_guard::retry_guard,
    types::{
        block::{ChunkIterator, INSERT_BLOCK_SIZE},
        query_result::stream_blocks::BlockStream,
        Cmd, Context, IntoOptions, OptionsSource, Packet, Query, QueryResult, SqlType,
    },
};
pub use crate::{
    errors::ConnectionError,
    pool::Pool,
    types::{block::Block, Options, Simple},
};

mod binary;
mod client_info;
mod connecting_stream;
/// Error types.
pub mod errors;
mod io;
/// Pool types.
pub mod pool;
mod retry_guard;
/// Clickhouse types.
pub mod types;

/// This macro is a convenient way to pass row into a block.
///
/// ```rust
/// # use clickhouse_rs::{Block, row, errors::Error};
/// # fn make_block() -> Result<(), Error> {
///       let mut block = Block::new();
///       block.push(row!{customer_id: 1, amount: 2, account_name: "foo"})?;
///       block.push(row!{customer_id: 4, amount: 4, account_name: "bar"})?;
///       block.push(row!{customer_id: 5, amount: 5, account_name: "baz"})?;
/// #     assert_eq!(block.row_count(), 3);
/// #     Ok(())
/// # }
/// # make_block().unwrap()
/// ```
///
/// If a column name has special characters, you can use the alternative syntax
/// with `=>` to pass an expression as column name:
///
/// ```rust
/// # use clickhouse_rs::{Block, row, errors::Error};
/// # fn make_block() -> Result<(), Error> {
///       let mut block = Block::new();
///       block.push(row!{"customer.id" => 1, amount: 2, "account.name" => "foo"})?;
///       block.push(row!{"customer.id" => 4, amount: 4, "account.name" => "bar"})?;
///       block.push(row!{"customer.id" => 5, amount: 5, "account.name" => "baz"})?;
/// #     assert_eq!(block.row_count(), 3);
/// #     Ok(())
/// # }
/// # make_block().unwrap()
/// ```
///
/// You can also use `Vec<(String, Value)>` to construct a row and insert it into a block:
///
/// ```rust
/// # use clickhouse_rs::{Block, errors::Error, types::Value};
/// # fn make_block() -> Result<(), Error> {
///       let mut block = Block::new();
///       for i in 1..10 {
///           let mut row = Vec::new();
///           for j in 1..10 {
///               row.push((format!("#{}", j), Value::from(i * j)));
///           }
///           block.push(row)?;
///       }
///       assert_eq!(block.row_count(), 9);
/// #     println!("{:?}", block);
/// #     Ok(())
/// # }
/// # make_block().unwrap()
/// ```
#[macro_export]
macro_rules! row {
    () => { $crate::types::RNil };
    ( $i:ident, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($i).into(), $i.into())
    };
    ( $i:ident ) => { row!($i: $i) };

    ( $k:ident: $v:expr ) => {
        $crate::types::RNil.put(stringify!($k).into(), $v.into())
    };

    ( $k:ident: $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($k).into(), $v.into())
    };

    ( $k:expr => $v:expr ) => {
        $crate::types::RNil.put($k.into(), $v.into())
    };

    ( $k:expr => $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put($k.into(), $v.into())
    };
}

#[macro_export]
macro_rules! try_opt {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(err),
        }
    };
}

#[doc(hidden)]
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
        Self::open(source, None).await
    }

    pub(crate) async fn open(source: OptionsSource, pool: Option<Pool>) -> Result<ClientHandle> {
        let options = try_opt!(source.get());
        let compress = options.compression;
        let timeout = options.connection_timeout;

        let context = Context {
            options: source.clone(),
            ..Context::default()
        };

        with_timeout(
            async move {
                let addr = match &pool {
                    None => &options.addr,
                    Some(p) => p.get_addr(),
                };

                info!("try to connect to {}", addr);
                if addr.port() == Some(8123) {
                    warn!("You should use port 9000 instead of 8123 because clickhouse-rs work through the binary interface.");
                }
                let mut stream = ConnectingStream::new(addr, &options).await?;
                stream.set_nodelay(options.nodelay)?;
                stream.set_keepalive(options.keepalive)?;

                let transport = ClickhouseTransport::new(stream, compress, pool.clone());
                let mut handle = ClientHandle {
                    inner: Some(transport),
                    context,
                    pool: match pool {
                        None => PoolBinding::None,
                        Some(p) => PoolBinding::Detached(p),
                    },
                };

                handle.hello().await?;
                Ok(handle)
            },
            timeout,
        )
        .await
    }
}

impl ClientHandle {
    pub(crate) async fn hello(&mut self) -> Result<()> {
        let context = self.context.clone();
        info!("[hello] -> {:?}", &context);

        let mut h = None;
        let mut info = None;
        let mut stream = self.inner.take().unwrap().call(Cmd::Hello(context.clone()));

        while let Some(packet) = stream.next().await {
            match packet {
                Ok(Packet::Hello(inner, server_info)) => {
                    info!("[hello] <- {:?}", &server_info);
                    h = Some(inner);
                    info = Some(server_info);
                }
                Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                Err(e) => return Err(Error::Io(e)),
                _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
            }
        }

        self.inner = h;
        self.context.server_info = info.unwrap();
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<()> {
        let timeout = try_opt!(self.context.options.get()).ping_timeout;

        with_timeout(
            async move {
                info!("[ping]");

                let mut h = None;

                let transport = self.get_inner()?.clear().await?;
                let mut stream = transport.call(Cmd::Ping);

                while let Some(packet) = stream.next().await {
                    match packet {
                        Ok(Packet::Pong(inner)) => {
                            info!("[pong]");
                            h = Some(inner);
                        }
                        Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                        Err(e) => return Err(Error::Io(e)),
                        _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
                    }
                }

                match h {
                    None => Err(Error::Connection(ConnectionError::Broken)),
                    Some(h) => {
                        self.inner = Some(h);
                        Ok(())
                    }
                }
            },
            timeout,
        )
        .await
    }

    /// Executes Clickhouse `query` on Conn.
    pub fn query<Q>(&mut self, sql: Q) -> QueryResult
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
    pub async fn execute<Q>(&mut self, sql: Q) -> Result<()>
    where
        Query: From<Q>,
    {
        let transport = self.execute_(sql).await?;
        self.inner = Some(transport);
        Ok(())
    }

    async fn execute_<Q>(&mut self, sql: Q) -> Result<ClickhouseTransport>
    where
        Query: From<Q>,
    {
        let timeout = try_opt!(self.context.options.get())
            .execute_timeout
            .unwrap_or_else(|| Duration::from_secs(0));
        let context = self.context.clone();
        let query = Query::from(sql);
        with_timeout(
            async {
                self.wrap_future(move |c| {
                    info!("[execute query] {}", query.get_sql());

                    let transport = c.get_inner();

                    async move {
                        let transport = transport?;
                        let mut h = None;

                        let transport = transport.clear().await?;
                        let mut stream = transport.call(Cmd::SendQuery(query, context.clone()));

                        while let Some(packet) = stream.next().await {
                            match packet {
                                Ok(Packet::Eof(inner)) => h = Some(inner),
                                Ok(Packet::Block(_))
                                | Ok(Packet::ProfileInfo(_))
                                | Ok(Packet::Progress(_)) => (),
                                Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                                Err(e) => return Err(Error::Io(e)),
                                _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
                            }
                        }

                        Ok(h.unwrap())
                    }
                })
                .await
            },
            timeout,
        )
        .await
    }

    /// Convenience method to insert block of data.
    pub async fn insert<Q, B>(&mut self, table: Q, block: B) -> Result<()>
    where
        Query: From<Q>,
        B: AsRef<Block>,
    {
        let query = Self::make_query(table, block.as_ref())?;
        let transport = self.insert_(query.clone(), block.as_ref()).await?;
        self.inner = Some(transport);
        Ok(())
    }

    async fn insert_(&mut self, query: Query, block: &Block) -> Result<ClickhouseTransport> {
        let timeout = try_opt!(self.context.options.get())
            .insert_timeout
            .unwrap_or_else(|| Duration::from_secs(0));

        let context = self.context.clone();

        with_timeout(
            async {
                self.wrap_future(move |c| {
                    info!("[insert]     {}", query.get_sql());
                    let transport = c.get_inner();

                    async move {
                        let transport = transport?.clear().await?;
                        let (transport, dst_block) =
                            Self::send_insert_query_(transport, context.clone(), query.clone())
                                .await?;
                        let casted_block = block.cast_to(&dst_block)?;
                        let mut chunks = casted_block.chunks(INSERT_BLOCK_SIZE);
                        let transport =
                            Self::insert_block_(transport, context.clone(), chunks.next().unwrap())
                                .await?;
                        Self::insert_tail_(transport, context, query, chunks).await
                    }
                })
                .await
            },
            timeout,
        )
        .await
    }

    async fn insert_tail_(
        mut transport: ClickhouseTransport,
        context: Context,
        query: Query,
        chunks: ChunkIterator<Simple>,
    ) -> Result<ClickhouseTransport> {
        for chunk in chunks {
            let (transport_, _) =
                Self::send_insert_query_(transport, context.clone(), query.clone()).await?;
            transport = Self::insert_block_(transport_, context.clone(), chunk).await?;
        }
        Ok(transport)
    }

    async fn send_insert_query_(
        transport: ClickhouseTransport,
        context: Context,
        query: Query,
    ) -> Result<(ClickhouseTransport, Block)> {
        let stream = transport.call(Cmd::SendQuery(query, context));
        let (transport, b) = stream.read_block().await?;
        let dst_block = b.unwrap();
        Ok((transport, dst_block))
    }

    async fn insert_block_(
        transport: ClickhouseTransport,
        context: Context,
        block: Block,
    ) -> Result<ClickhouseTransport> {
        let send_cmd = Cmd::Union(
            Box::new(Cmd::SendData(block, context.clone())),
            Box::new(Cmd::SendData(Block::default(), context)),
        );
        let (transport, _) = transport.call(send_cmd).read_block().await?;
        Ok(transport)
    }

    fn make_query<Q>(table: Q, block: &Block) -> Result<Query>
    where
        Query: From<Q>,
    {
        let mut names: Vec<_> = Vec::with_capacity(block.as_ref().column_count());
        for column in block.as_ref().columns() {
            names.push(try_opt!(column_name_to_string(column.name())));
        }
        let fields = names.join(", ");
        Ok(Query::from(table).map_sql(|table| format!("INSERT INTO {table} ({fields}) VALUES")))
    }

    pub(crate) async fn wrap_future<T, R, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Self) -> R + Send,
        R: Future<Output = Result<T>>,
        T: 'static,
    {
        let ping_before_query = try_opt!(self.context.options.get()).ping_before_query;

        if ping_before_query {
            self.check_connection().await?;
        }
        f(self).await
    }

    pub(crate) fn wrap_stream<'a, F>(&'a mut self, f: F) -> BoxStream<'a, Result<Block>>
    where
        F: (FnOnce(&'a mut Self) -> Result<BlockStream<'a>>) + Send + 'static,
    {
        let ping_before_query = match self.context.options.get() {
            Ok(val) => val.ping_before_query,
            Err(err) => return Box::pin(stream::once(future::err(err))),
        };

        if ping_before_query {
            let fut: BoxFuture<'a, BoxStream<'a, Result<Block>>> = Box::pin(async move {
                let inner: BoxStream<'a, Result<Block>> = match self.check_connection().await {
                    Ok(_) => match f(self) {
                        Ok(s) => Box::pin(s),
                        Err(err) => Box::pin(stream::once(future::err(err))),
                    },
                    Err(err) => Box::pin(stream::once(future::err(err))),
                };
                inner
            });

            Box::pin(fut.flatten_stream())
        } else {
            match f(self) {
                Ok(s) => Box::pin(s),
                Err(err) => Box::pin(stream::once(future::err(err))),
            }
        }
    }

    /// Check connection and try to reconnect if necessary.
    pub async fn check_connection(&mut self) -> Result<()> {
        self.pool.detach();

        let source = self.context.options.clone();
        let pool = self.pool.clone();

        let (send_retries, retry_timeout) = {
            let options = try_opt!(source.get());
            (options.send_retries, options.retry_timeout)
        };

        retry_guard(self, &source, pool.into(), send_retries, retry_timeout).await?;

        if !self.pool.is_attached() && self.pool.is_some() {
            self.pool.attach();
        }

        Ok(())
    }

    pub(crate) fn set_inside(&self, value: bool) {
        if let Some(ref inner) = self.inner {
            inner.set_inside(value);
        } else {
            unreachable!()
        }
    }

    fn get_inner(&mut self) -> Result<ClickhouseTransport> {
        self.inner
            .take()
            .ok_or_else(|| Error::Connection(ConnectionError::Broken))
    }
}

fn column_name_to_string(name: &str) -> Result<String> {
    if name.chars().all(|ch| ch.is_numeric()) {
        return Ok(name.to_string());
    }

    if name.chars().any(|ch| ch == '`') {
        let err = format!("Column name {name:?} shouldn't contains backticks.");
        return Err(Error::Other(err.into()));
    }

    Ok(format!("`{name}`"))
}

#[cfg(feature = "async_std")]
async fn with_timeout<F, T>(future: F, duration: Duration) -> F::Output
where
    F: Future<Output = Result<T>>,
{
    use async_std::io;
    use futures_util::future::TryFutureExt;

    io::timeout(duration, future.map_err(Into::into))
        .map_err(Into::into)
        .await
}

#[cfg(not(feature = "async_std"))]
async fn with_timeout<F, T>(future: F, timeout: Duration) -> F::Output
where
    F: Future<Output = Result<T>>,
{
    tokio::time::timeout(timeout, future).await?
}

#[cfg(test)]
pub(crate) mod test_misc {
    use crate::*;
    use std::env;

    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref DATABASE_URL: String = env::var("DATABASE_URL").unwrap_or_else(|_| {
            "tcp://localhost:9000?compression=lz4&ping_timeout=1s&retry_timeout=2s".into()
        });
    }

    #[test]
    fn test_column_name_to_string() {
        assert_eq!(column_name_to_string("id").unwrap(), "`id`");
        assert_eq!(column_name_to_string("234").unwrap(), "234");
        assert_eq!(column_name_to_string("ns:attr").unwrap(), "`ns:attr`");
        assert!(column_name_to_string("`").is_err());
    }
}
