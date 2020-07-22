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
//! - `query_timeout` - Timeout for queries (defaults to `180 sec`).
//! - `query_block_timeout` - Timeout for each block in a query (defaults to `180 sec`).
//! - `insert_timeout` - Timeout for inserts (defaults to `180 sec`).
//! - `execute_timeout` - Timeout for execute (defaults to `180 sec`).
//!
//! SSL/TLS parameters:
//!
//! - `secure` - establish secure connection (defaults is `false`).
//! - `skip_verify` - skip certificate verification (defaults is `false`).
//!
//! example:
//! ```url
//! tcp://user:password@host:9000/clicks?compression=lz4&ping_timeout=42ms
//! ```
//!
//! ### Example
//!
//! ```rust
//! use futures::Future;
//! use clickhouse_rs::{Pool, types::Block};
//! # use std::env;
//!
//!  let ddl = "
//!      CREATE TABLE IF NOT EXISTS payment (
//!          customer_id  UInt32,
//!          amount       UInt32,
//!          account_name Nullable(FixedString(3))
//!      ) Engine=Memory";
//!
//!  let block = Block::new()
//!      .column("customer_id",  vec![1_u32,  3,  5,  7,  9])
//!      .column("amount",       vec![2_u32,  4,  6,  8, 10])
//!      .column("account_name", vec![Some("foo"), None, None, None, Some("bar")]);
//!
//!  # let database_url = env::var("DATABASE_URL").unwrap_or("tcp://localhost:9000?compression=lz4".into());
//!  let pool = Pool::new(database_url);
//!
//!  let done = pool
//!     .get_handle()
//!     .and_then(move |c| c.execute(ddl))
//!     .and_then(move |c| c.insert("payment", block))
//!     .and_then(move |c| c.query("SELECT * FROM payment").fetch_all())
//!     .and_then(move |(_, block)| {
//!         for row in block.rows() {
//!             let id: u32     = row.get("customer_id")?;
//!             let amount: u32 = row.get("amount")?;
//!             let name: Option<&str>  = row.get("account_name")?;
//!             println!("Found payment {}: {} {:?}", id, amount, name);
//!         }
//!         Ok(())
//!     })
//!     .map_err(|err| eprintln!("database error: {}", err));
//!
//! tokio::run(done)
//! ```

#![recursion_limit = "1024"]

extern crate byteorder;
extern crate chrono;
extern crate chrono_tz;
extern crate clickhouse_rs_cityhash_sys;
extern crate core;
#[macro_use]
extern crate futures;
extern crate hostname;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate lz4;
#[cfg(test)]
extern crate rand;
extern crate tokio;
extern crate tokio_timer;
extern crate url;

use std::{fmt, time::Duration};

use futures::{Future, Stream};
use tokio::prelude::*;

pub use crate::pool::Pool;
use crate::{
    connecting_stream::ConnectingStream,
    errors::{DriverError, Error},
    io::{BoxFuture, BoxStream, ClickhouseTransport},
    pool::PoolBinding,
    retry_guard::RetryGuard,
    types::{
        set_exception_handle, Block, Cmd, Complex, Context, Either, IntoOptions, Options,
        OptionsSource, Packet, Query, QueryResult,
    },
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
/// # use clickhouse_rs::{types::Block, row, errors::Error};
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
/// you can also use `Vec<(String, Value)>` to construct row to insert into a block:
///
/// ```rust
/// # use clickhouse_rs::{types::Block, errors::Error, types::Value};
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

macro_rules! try_opt {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Either::Left(future::err(err)),
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
    pub fn connect(options: Options) -> impl Future<Item = ClientHandle, Error = Error> {
        Self::open(&options.into_options_src(), None)
    }

    pub(crate) fn open(
        source: &OptionsSource,
        pool: Option<Pool>,
    ) -> impl Future<Item = ClientHandle, Error = Error> {
        let options = try_opt!(source.get()).as_ref().to_owned();
        let compress = options.compression;
        let timeout = options.connection_timeout;

        let context = Context {
            options: source.clone(),
            ..Context::default()
        };

        Either::Right(
            future::lazy(move || {
                let addr = match &pool {
                    None => &options.addr,
                    Some(p) => p.get_addr(),
                };

                info!("try to connect to {}", addr);
                ConnectingStream::new(addr, &options)
                    .and_then(move |mut stream| {
                        stream.set_nodelay(options.nodelay)?;
                        stream.set_keepalive(options.keepalive)?;

                        let transport = ClickhouseTransport::new(stream, compress, pool);
                        Ok(ClientHandle {
                            inner: Some(transport),
                            context,
                            pool: PoolBinding::None,
                        })
                    })
                    .map_err(Into::into)
                    .and_then(ClientHandle::hello)
                    .timeout(timeout)
                    .map_err(Error::from)
            }),
        )
    }
}

impl ClientHandle {
    fn hello(mut self) -> impl Future<Item = Self, Error = Error> {
        let context = self.context.clone();
        let pool = self.pool.clone();
        debug!("[hello] -> {:?}", &context);

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
                    let client = Self {
                        inner: Some(inner),
                        context,
                        pool: pool.clone(),
                    };
                    future::ok::<_, Error>(Some(client))
                }
                Packet::Exception(e, _) => future::err::<_, Error>(Error::Server(e)),
                _ => future::err::<_, Error>(Error::Driver(DriverError::UnexpectedPacket)),
            })
            .map(Option::unwrap)
    }

    pub fn ping(mut self) -> impl Future<Item = Self, Error = Error> {
        let context = self.context.clone();
        let timeout = try_opt!(self.context.options.get()).ping_timeout;

        let pool = self.pool.clone();
        info!("[ping]");
        let fut = self
            .inner
            .take()
            .unwrap()
            .call(Cmd::Ping)
            .fold(None, move |_, packet| match packet {
                Packet::Pong(inner) => {
                    let client = Self {
                        inner: Some(inner),
                        context: context.clone(),
                        pool: pool.clone(),
                    };
                    info!("[pong]");
                    future::ok::<_, Error>(Some(client))
                }
                Packet::Exception(mut exception, transport) => {
                    set_exception_handle(&mut exception, transport, context.clone(), pool.clone());
                    future::err::<_, Error>(Error::Server(exception))
                }
                _ => future::err::<_, Error>(Error::Driver(DriverError::UnexpectedPacket)),
            })
            .map(Option::unwrap)
            .timeout(timeout)
            .map_err(Error::from);

        Either::Right(fut)
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
    #[deprecated(since = "0.1.7", note = "please use query(sql).fetch_all() instead")]
    pub fn query_all<Q>(self, sql: Q) -> BoxFuture<(Self, Block<Complex>)>
    where
        Query: From<Q>,
    {
        self.query(sql).fetch_all()
    }

    /// Convenience method to prepare and execute a single SQL statement.
    pub fn execute<Q>(self, sql: Q) -> impl Future<Item = Self, Error = Error>
    where
        Query: From<Q>,
    {
        let context = self.context.clone();
        let pool = self.pool.clone();
        let timeout = try_opt!(context.options.get()).execute_timeout;

        let query = Query::from(sql);
        let fut = self.wrap_future(move |mut c| {
            info!("[execute]    {}", query.get_sql());

            let future = c
                .inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()))
                .fold(None, move |acc, packet| match packet {
                    Packet::Eof(inner) => {
                        let client = Self {
                            inner: Some(inner),
                            context: context.clone(),
                            pool: pool.clone(),
                        };
                        future::ok::<_, Error>(Some(client))
                    }
                    Packet::Block(_) | Packet::ProfileInfo(_) | Packet::Progress(_) => {
                        future::ok::<_, Error>(acc)
                    }
                    Packet::Exception(mut exception, transport) => {
                        set_exception_handle(
                            &mut exception,
                            transport,
                            context.clone(),
                            pool.clone(),
                        );
                        future::err::<_, Error>(Error::Server(exception))
                    }
                    _ => future::err::<_, Error>(Error::Driver(DriverError::UnexpectedPacket)),
                })
                .map(Option::unwrap);

            with_timeout(future, timeout)
        });

        Either::Right(fut)
    }

    /// Convenience method to insert block of data.
    pub fn insert<Q>(self, table: Q, block: Block) -> impl Future<Item = Self, Error = Error>
    where
        Query: From<Q>,
    {
        let mut names: Vec<_> = Vec::with_capacity(block.column_count());
        for column in block.columns() {
            names.push(try_opt!(column_name_to_string(column.name())));
        }
        let fields = names.join(", ");

        let query = Query::from(table)
            .map_sql(|table| format!("INSERT INTO {} ({}) VALUES", table, fields));

        let context = self.context.clone();
        let pool = self.pool.clone();
        let timeout = try_opt!(context.options.get()).insert_timeout;

        let fut = self.wrap_future(move |mut c| {
            info!("[insert]     {}", query.get_sql());

            let future = c
                .inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()))
                .read_block(context.clone(), pool.clone())
                .and_then(move |(mut c, b)| -> BoxFuture<Self> {
                    let dst_block = b.unwrap();

                    let casted_block = match block.cast_to(&dst_block) {
                        Ok(value) => value,
                        Err(err) => return Box::new(future::err::<Self, Error>(err)),
                    };

                    let send_cmd = Cmd::Union(
                        Box::new(Cmd::SendData(casted_block, context.clone())),
                        Box::new(Cmd::SendData(Block::default(), context.clone())),
                    );

                    Box::new(
                        c.inner
                            .take()
                            .unwrap()
                            .call(send_cmd)
                            .read_block(context, pool)
                            .map(|(c, _)| c),
                    )
                });

            with_timeout(future, timeout)
        });

        Either::Right(fut)
    }

    pub(crate) fn wrap_future<T, R, F>(self, f: F) -> impl Future<Item = T, Error = Error>
    where
        F: FnOnce(Self) -> R + Send + 'static,
        R: Future<Item = T, Error = Error> + Send + 'static,
        T: Send + 'static,
    {
        let ping_before_query = try_opt!(self.context.options.get()).ping_before_query;

        let fut = if ping_before_query {
            Either::Left(self.check_connection().and_then(move |c| Box::new(f(c))))
        } else {
            Either::Right(f(self))
        };

        Either::Right(fut)
    }

    pub(crate) fn wrap_stream<T, R, F>(self, f: F) -> BoxStream<T>
    where
        F: FnOnce(Self) -> R + Send + 'static,
        R: Stream<Item = T, Error = Error> + Send + 'static,
        T: Send + 'static,
    {
        let ping_before_query = match self.context.options.get() {
            Ok(val) => val.ping_before_query,
            Err(err) => return Box::new(stream::once(Err(err))),
        };

        if ping_before_query {
            let fut = self
                .check_connection()
                .and_then(move |c| future::ok(Box::new(f(c))))
                .flatten_stream();
            Box::new(fut)
        } else {
            Box::new(f(self))
        }
    }

    /// Check connection and try to reconnect if necessary.
    pub fn check_connection(mut self) -> impl Future<Item = Self, Error = Error> {
        let pool: Option<Pool> = self.pool.clone().into();
        self.pool.detach();

        let source = self.context.options.clone();

        let (send_retries, retry_timeout) = match source.get() {
            Ok(val) => (val.send_retries, val.retry_timeout),
            Err(err) => return Either::Left(future::err(err)),
        };

        let reconnect = move || -> BoxFuture<Self> {
            warn!("[reconnect]");
            match pool.clone() {
                None => Box::new(Client::open(&source, None)),
                Some(p) => Box::new(p.get_handle()),
            }
        };

        let fut = RetryGuard::new(
            self,
            |c| Box::new(c.ping()),
            reconnect,
            send_retries,
            retry_timeout,
        )
        .and_then(|mut c| {
            if !c.pool.is_attached() && c.pool.is_some() {
                c.pool.attach();
            }
            Ok(c)
        });

        Either::Right(fut)
    }

    pub(crate) fn set_inside(&self, value: bool) {
        if let Some(ref inner) = self.inner {
            inner.set_inside(value);
        } else {
            unreachable!()
        }
    }
}

fn column_name_to_string(name: &str) -> Result<String, Error> {
    if name.chars().all(|ch| ch.is_alphanumeric()) {
        return Ok(name.to_string());
    }

    if name.chars().any(|ch| ch == '`') {
        return Err(Error::Other("Column name shouldn't contains backticks.".into()));
    }

    Ok(format!("`{}`", name))
}

pub(crate) fn with_timeout<F>(
    f: F,
    timeout: Option<Duration>,
) -> impl Future<Item = F::Item, Error = Error>
where
    F: Future<Error = Error> + Send + 'static,
{
    if let Some(timeout) = timeout {
        Either::Left(f.timeout(timeout).map_err(|err| err.into()))
    } else {
        Either::Right(f)
    }
}

#[cfg(test)]
mod test_misc {
    use crate::*;
    use std::env;

    lazy_static! {
        pub static ref DATABASE_URL: String = env::var("DATABASE_URL").unwrap_or_else(|_| {
            "tcp://localhost:9000?compression=lz4&ping_timeout=5s&retry_timeout=5s".into()
        });
    }

    #[test]
    fn test_column_name_to_string() {
        assert_eq!(column_name_to_string("id").unwrap(), "id");
        assert_eq!(column_name_to_string("ns:attr").unwrap(), "`ns:attr`");
        assert!(column_name_to_string("`").is_err());
    }
}
