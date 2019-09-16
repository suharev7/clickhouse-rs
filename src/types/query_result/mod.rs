use std::{sync::Arc, future::Future};

use futures_core::stream::BoxStream;
use futures_util::{
    future,
    stream::{self, StreamExt}
};
use log::info;

use crate::{
    ClientHandle,
    errors::{DriverError, Error, Result},
    io::ClickhouseTransport,
    types::{
        Block,
        block::BlockRef,
        Cmd,
        Packet,
        Query,
        query_result::stream_blocks::BlockStream,
        Row,
        Rows
    },
};

use self::fold_block::FoldBlock;

mod fold_block;
mod stream_blocks;

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
    /// # use std::env;
    /// # use clickhouse_rs::{Pool, errors::Result};
    /// # use futures_util::future;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let ret: Result<()> = rt.block_on(async {
    /// #     let database_url = env::var("DATABASE_URL")
    /// #         .unwrap_or("tcp://localhost:9000?compression=lz4".into());
    /// #     let pool = Pool::new(database_url);
    ///       pool.get_handle().await?
    ///           .query("SELECT number FROM system.numbers LIMIT 10000000")
    ///           .try_fold(0, |acc, row| {
    ///                let number: u64 = row.get("number").unwrap();
    ///                future::ready(Ok(acc + number))
    ///           }).await?;
    /// #     Ok(())
    /// # });
    /// # ret.unwrap()
    /// ```
    pub async fn try_fold<F, T, Fut>(self, init: T, f: F) -> Result<(ClientHandle, T)>
    where
        F: Fn(T, Row) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Unpin,
        T: Send + 'static,
    {
        let func_ptr = Arc::new(f);

        let (c, r) = self.try_fold_blocks(init, move  |acc, block| {
            FoldBlock::new(block, acc, func_ptr.clone())
        }).await?;

        Ok((c, r))
    }

    /// Fetch data from table. It returns a block that contains all rows.
    pub async fn fetch_all(self) -> Result<(ClientHandle, Block)> {
        let (h, blocks) = self.try_fold_blocks(Vec::new(), |mut blocks, block| {
            if !block.is_empty() {
                blocks.push(block);
            }
            future::ready(Ok(blocks))
        }).await?;
        Ok((h, Block::concat(blocks.as_slice())))
    }

    /// Method that applies a function to each block, producing a single, final value.
    pub async fn try_fold_blocks<F, T, Fut>(self, init: T, f: F) -> Result<(ClientHandle, T)>
    where
        F: (Fn(T, Block) -> Fut) + Send + 'static,
        Fut: Future<Output = Result<T>> + Unpin,
        T: Send + 'static,
    {
        let context = self.client.context.clone();
        let pool = self.client.pool.clone();
        let release_pool = self.client.pool.clone();

        let acc = (None, init, f);
        let ret = self.try_fold_packets(acc, async move |(h, acc, f), packet| {
            match packet {
                Packet::Block(block) => {
                    let a = f(acc, block).await?;
                    Ok((h, a, f))
                }
                Packet::Eof(inner) => Ok((Some(inner), acc, f)),
                Packet::ProfileInfo(_) | Packet::Progress(_) => Ok((h, acc, f)),
                Packet::Exception(exception) => Err(Error::Server(exception)),
                _ => Err(Error::Driver(DriverError::UnexpectedPacket)),
            }
        }).await;

        match ret {
            Ok((transport, t, _)) => {
                let client = ClientHandle {
                    inner: Some(transport.unwrap()),
                    context: context.clone(),
                    pool: pool.clone(),
                };

                Ok((client, t))
            }
            Err(err) => {
                release_pool.release_conn();
                Err(err)
            }
        }
    }

    async fn try_fold_packets<F, T, Fut>(self, init: T, f: F) -> Result<T>
    where
        F: (Fn(T, Packet<ClickhouseTransport>) -> Fut) + Send + 'static,
        Fut: Future<Output = Result<T>>,
        T: Send + 'static,
    {
        let context = self.client.context.clone();
        let query = self.query;

        self.client.wrap_future(async move |mut c| {
            info!("[send query] {}", query.get_sql());
            c.pool.detach();

            let mut stream = c.inner
                .take()
                .unwrap()
                .call(Cmd::SendQuery(query, context.clone()));

            let mut acc = init;
            while let Some(packet) = stream.next().await {
                acc = f(acc, packet?).await?;
            }

            Ok(acc)
        }).await
    }

    /// Method that produces a stream of blocks containing rows
    ///
    /// example:
    ///
    /// ```rust
    /// # use std::env;
    /// # use clickhouse_rs::{Pool, errors::Result};
    /// # use futures_util::{future, TryStreamExt};
    /// #
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let ret: Result<()> = rt.block_on(async {
    /// #
    /// #     let database_url = env::var("DATABASE_URL")
    /// #         .unwrap_or("tcp://localhost:9000?compression=lz4".into());
    /// #
    /// #     let sql_query = "SELECT number FROM system.numbers LIMIT 100000";
    /// #     let pool = Pool::new(database_url);
    /// #
    ///       pool.get_handle().await?
    ///           .query(sql_query)
    ///           .stream_blocks()
    ///           .try_for_each(|block| {
    ///               println!("{:?}\nblock counts: {} rows", block, block.row_count());
    ///               future::ready(Ok(()))
    ///           }).await?;
    /// #     Ok(())
    /// # });
    /// # ret.unwrap()
    /// ```
    pub fn stream_blocks(self) -> BoxStream<'static, Result<Block>> {
        let query = self.query;

        self.client.wrap_stream(move |mut c| {
            info!("[send query] {}", query.get_sql());
            c.pool.detach();

            let context = c.context.clone();
            let pool = c.pool.clone();

            BlockStream::new(
                c.inner
                    .take()
                    .unwrap()
                    .call(Cmd::SendQuery(query, context.clone())),
                context,
                pool,
            )
        })
    }

    /// Method that produces a stream of rows
    pub fn stream(self) -> BoxStream<'static, Result<Row<'static>>> {
        Box::pin(self.stream_blocks()
            .map(|block_ret| {
                let result: BoxStream<'static, Result<Row<'static>>> =
                    match block_ret {
                        Ok(block) => {
                            let block = Arc::new(block);
                            let block_ref = BlockRef::Owned(block);

                            Box::pin(stream::iter(Rows { row: 0, block_ref })
                                .map(|row| -> Result<Row<'static>> { Ok(row) }))
                        }
                        Err(err) => Box::pin(stream::once(future::err(err))),
                    };
                result
            }).flatten())
    }
}
