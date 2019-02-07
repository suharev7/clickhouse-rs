use std::sync::Arc;

use tokio::prelude::*;

use crate::{
    errors::{DriverError, Error},
    io::{BoxFuture, ClickhouseTransport},
    types::{Block, Cmd, Packet, Query, Row},
    ClientHandle,
};

use self::{
    fold_block::FoldBlock,
    either::Either,
};

mod fold_block;
mod either;

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
    ///         c.query("SELECT * FROM system.numbers LIMIT 10")
    ///             .fold(0, |acc, row| {
    ///                 let number: u64 = row.get("number")?;
    ///                 Ok(acc + number)
    ///             })
    ///     })
    /// #   .map(|_| ())
    /// #   .map_err(|err| eprintln!("database error: {}", err));
    /// # tokio::run(done)
    /// ```
    pub fn fold<F, T, Fut>(self, init: T, f: F) -> BoxFuture<(ClientHandle, T)>
    where
        F: Fn(T, Row) -> Fut + Send + Sync + 'static,
        Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
        Fut::Future: Send,
        T: Send + 'static,
    {
        let func_ptr = Arc::new(f);

        self.fold_blocks(init, move |acc, block| {
            wrap_future(FoldBlock::new(block, acc, func_ptr.clone()))
        })
    }

    /// Fetch data from table. It returns a block that contains all rows.
    pub fn fetch_all(self) -> BoxFuture<(ClientHandle, Block)> {
        wrap_future(
            self.fold_blocks(Vec::new(), |mut blocks, block| {
                if !block.is_empty() {
                    blocks.push(block);
                }
                Ok(blocks)
            })
            .map(|(h, blocks)| (h, Block::concat(blocks.as_slice()))),
        )
    }

    /// Method that applies a function to each block, producing a single, final value.
    pub fn fold_blocks<F, T, Fut>(self, init: T, f: F) -> BoxFuture<(ClientHandle, T)>
    where
        F: Fn(T, Block) -> Fut + Send + 'static,
        Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
        Fut::Future: Send,
        T: Send + 'static,
    {
        let context = self.client.context.clone();
        let pool = self.client.pool.clone();
        let release_pool = self.client.pool.clone();

        let acc = (None, init);
        Box::new(
            self.fold_packets(acc, move |(h, acc), packet| match packet {
                Packet::Block(b) => Either::Left(f(acc, b).into_future().map(move |a| (h, a))),
                Packet::Eof(inner) => {
                    Either::Right(future::ok((
                        Some(ClientHandle {
                            inner: Some(inner),
                            context: context.clone(),
                            pool: pool.clone(),
                        }),
                        acc,
                    )))
                }
                Packet::ProfileInfo(_) | Packet::Progress(_) => Either::Right(future::ok((h, acc))),
                Packet::Exception(exception) => Either::Right(future::err(Error::Server(exception))),
                _ => Either::Right(future::err(Error::Driver(DriverError::UnexpectedPacket))),
            })
            .map(|(c, t)| (c.unwrap(), t))
            .map_err(move |err| {
                release_pool.release_conn();
                err
            }),
        )
    }

    fn fold_packets<F, T, Fut>(self, init: T, f: F) -> BoxFuture<T>
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
}

fn wrap_future<T, F>(future: F) -> BoxFuture<T>
where
    F: Future<Item = T, Error = Error> + Send + 'static,
{
    Box::new(future)
}
