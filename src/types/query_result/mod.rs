use std::{marker, sync::Arc};

use tokio::prelude::*;

use crate::{
    errors::{DriverError, Error, ServerError},
    io::{BoxFuture, BoxStream, ClickhouseTransport},
    types::{
        block::BlockRef, query_result::stream_blocks::BlockStream, Block, Cmd,
        Complex, Packet, Query, Row, Rows, Simple, either::Either, Context,
    },
    pool::PoolBinding,
    ClientHandle,
};

use self::fold_block::FoldBlock;

mod fold_block;
mod stream_blocks;

macro_rules! try_opt_stream {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Box::new(stream::once(Err(err))),
        }
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

/// Result of a query or statement execution.
pub struct QueryResult {
    pub(crate) client: ClientHandle,
    pub(crate) query: Query,
}

impl QueryResult {
    /// Method that applies a function to each row, producing a single, final value.
    ///
    /// example:
    /// ```rust
    /// # extern crate clickhouse_rs;
    /// # extern crate futures;
    /// # use futures::Future;
    /// # use clickhouse_rs::{Pool, types::Block};
    /// # use std::env;
    /// # let database_url = env::var("DATABASE_URL").unwrap_or("tcp://localhost:9000?compression=lz4".into());
    /// # let pool = Pool::new(database_url);
    /// # let done =
    /// pool.get_handle()
    ///     .and_then(|c| {
    ///         c.query("SELECT number FROM system.numbers LIMIT 10000000")
    ///             .fold(0, |acc, row| {
    ///                 let number: u64 = row.get("number")?;
    ///                 Ok(acc + number)
    ///             })
    ///     })
    /// #   .map(|_| ())
    /// #   .map_err(|err| eprintln!("database error: {}", err));
    /// # tokio::run(done)
    /// ```
    pub fn fold<F, T, Fut>(self, init: T, f: F) -> impl Future<Item=(ClientHandle, T), Error=Error>
        where
            F: Fn(T, Row<Simple>) -> Fut + Send + Sync + 'static,
            Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
            Fut::Future: Send,
            T: Send + 'static,
    {
        let func_ptr = Arc::new(f);

        self.fold_blocks(init, move |acc, block| FoldBlock::new(block, acc, func_ptr.clone()))
    }

    /// Fetch data from table. It returns a block that contains all rows.
    pub fn fetch_all(self) -> BoxFuture<(ClientHandle, Block<Complex>)> {
        Box::new(
            self.fold_blocks(Vec::new(), |mut blocks, block| {
                if !block.is_empty() {
                    blocks.push(block);
                }
                Ok(blocks)
            })
                .map_err(Error::from)
                .map(|(h, blocks)| (h, Block::concat(blocks.as_slice()))),
        )
    }

    /// Method that applies a function to each block, producing a single, final value.
    pub fn fold_blocks<F, T, Fut>(self, init: T, f: F) -> impl Future<Item=(ClientHandle, T), Error=Error>
        where
            F: Fn(T, Block) -> Fut + Send + 'static,
            Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
            Fut::Future: Send,
            T: Send + 'static,
    {
        let timeout = try_opt!(self.client.context.options.get()).query_timeout;
        let context = self.client.context.clone();
        let pool = self.client.pool.clone();

        let acc = (None, init);

        let future = self.fold_packets(acc, move |(h, acc), packet| match packet {
            Packet::Block(b) => {
                if b.is_empty() {
                    Either::Right(future::ok((h, acc)))
                } else {
                    Either::Left(f(acc, b).into_future().map(move |a| (h, a)))
                }
            }
            Packet::Eof(inner) => Either::Right(future::ok((
                Some(ClientHandle {
                    inner: Some(inner),
                    context: context.clone(),
                    pool: pool.clone(),
                }),
                acc,
            ))),
            Packet::ProfileInfo(_) | Packet::Progress(_) => Either::Right(future::ok((h, acc))),
            Packet::Exception(mut exception, transport) => {
                set_exception_handle(&mut exception, transport, context.clone(), pool.clone());
                Either::Right(future::err(Error::Server(exception)))
            },
            _ => Either::Right(future::err(Error::Driver(DriverError::UnexpectedPacket))),
        });

        let fut = if let Some(timeout) = timeout {
            Either::Left(
                future
                    .map(|(c, t)| (c.unwrap(), t))
                    .timeout(timeout)
                    .map_err(move |err| err.into()),
            )
        } else {
            Either::Right(future.map(|(c, t)| (c.unwrap(), t)))
        };

        Either::Right(fut)
    }

    fn fold_packets<F, T, Fut>(self, init: T, f: F) -> impl Future<Item=T, Error=Error>
        where
            F: Fn(T, Packet<ClickhouseTransport>) -> Fut + Send + 'static,
            Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
            Fut::Future: Send,
            T: Send + 'static,
    {
        let context = self.client.context.clone();
        let query = self.query;

        self.client.wrap_future(move |mut c| {
            info!("[send query] {}", query.get_sql());
            c.pool.detach();
            c.inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()))
                .fold(init, f)
        })
    }

    /// Method that produces a stream of blocks containing rows
    ///
    /// example:
    /// ```rust
    /// # extern crate clickhouse_rs;
    /// # extern crate futures;
    /// # use futures::{Future, Stream};
    /// # use clickhouse_rs::Pool;
    /// # use std::env;
    /// # let database_url = env::var("DATABASE_URL").unwrap_or("tcp://localhost:9000?compression=lz4".into());
    /// # let pool = Pool::new(database_url);
    /// # let done =
    ///  pool.get_handle()
    ///      .and_then(|c| {
    /// #        let sql_query = "SELECT number FROM system.numbers LIMIT 100000";
    ///          c.query(sql_query)
    ///              .stream_blocks()
    ///              .for_each(|block| {
    ///                  println!("{:?}\nblock counts: {} rows", block, block.row_count());
    /// #                Ok(())
    ///              })
    ///      })
    /// #    .map(|_| ())
    /// #    .map_err(|err| eprintln!("database error: {}", err));
    /// # tokio::run(done)
    /// ```
    pub fn stream_blocks(self) -> BoxStream<Block> {
        let query = self.query;
        let timeout = try_opt_stream!(self.client.context.options.get()).query_block_timeout;

        self.client.wrap_stream(move |mut c| -> BoxStream<Block> {
            info!("[send query] {}", query.get_sql());

            c.pool.detach();

            let context = c.context.clone();
            let pool = c.pool.clone();

            let stream = BlockStream::new(
                c.inner
                    .take()
                    .unwrap()
                    .call(Cmd::SendQuery(query, context.clone())),
                context,
                pool,
            );

            if let Some(timeout) = timeout {
                Box::new(stream.timeout(timeout).map_err(|err| err.into()))
            } else {
                Box::new(stream)
            }
        })
    }

    /// Method that produces a stream of rows
    pub fn stream_rows(self) -> BoxStream<Row<'static, Simple>> {
        Box::new(
            self.stream_blocks()
                .map(Arc::new)
                .map(|block| {
                    let block_ref = BlockRef::Owned(block);
                    stream::iter_ok(Rows {
                        row: 0,
                        block_ref,
                        kind: marker::PhantomData,
                    })
                })
                .flatten(),
        )
    }
}

const CANNOT_PARSE_DATE: u32 = 38;
const CANNOT_PARSE_DATETIME: u32 = 41;
const UNKNOWN_FUNCTION: u32 = 46;
const UNKNOWN_IDENTIFIER: u32 = 47;
const SYNTAX_ERROR: u32 = 62;
const UNKNOWN_AGGREGATE_FUNCTION: u32 = 63;
const ILLEGAL_KEY_OF_AGGREGATION: u32 = 67;
const ARGUMENT_OUT_OF_BOUND: u32 = 69;
const CANNOT_CONVERT_TYPE: u32 = 70;
const CANNOT_WRITE_AFTER_END_OF_BUFFER: u32 = 71;
const CANNOT_PARSE_NUMBER: u32 = 72;
const ILLEGAL_DIVISION: u32 = 153;
const READONLY: u32 = 164;
const BARRIER_TIMEOUT: u32 = 335;
const TIMEOUT_EXCEEDED: u32 = 159;
const SOCKET_TIMEOUT: u32 = 209;
const INVALID_SESSION_TIMEOUT: u32 = 374;

pub(crate) fn set_exception_handle(exception: &mut ServerError, transport: Option<ClickhouseTransport>, context: Context, pool: PoolBinding) {
    if let Some(transport) = transport {
        match exception.code {
            CANNOT_PARSE_DATE |
            CANNOT_PARSE_DATETIME |
            UNKNOWN_FUNCTION |
            UNKNOWN_IDENTIFIER |
            SYNTAX_ERROR |
            UNKNOWN_AGGREGATE_FUNCTION |
            ILLEGAL_KEY_OF_AGGREGATION |
            ARGUMENT_OUT_OF_BOUND |
            CANNOT_CONVERT_TYPE |
            CANNOT_WRITE_AFTER_END_OF_BUFFER |
            CANNOT_PARSE_NUMBER |
            ILLEGAL_DIVISION |
            READONLY |
            BARRIER_TIMEOUT |
            TIMEOUT_EXCEEDED |
            SOCKET_TIMEOUT |
            INVALID_SESSION_TIMEOUT => {
                let client = ClientHandle {
                    inner: Some(transport),
                    context: context.clone(),
                    pool: pool.clone(),
                };
                exception.handle = Some(Arc::new(client))
            }
            _ => ()
        }
    }
}
