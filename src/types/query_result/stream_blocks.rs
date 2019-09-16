use std::{
    pin::Pin,
    task::{self, Poll},
};

use futures_core::Stream;
use futures_util::StreamExt;

use crate::{
    ClientHandle,
    errors::{DriverError, Error, Result},
    io::transport::PacketStream,
    pool::PoolBinding,
    types::{Block, Context, Packet},
};

pub(crate) struct BlockStream {
    inner: PacketStream,
    rest: Option<(Context, PoolBinding)>,
    eof: bool,
    block_index: usize,
}

impl BlockStream {
    pub(crate) fn new(inner: PacketStream, context: Context, pool: PoolBinding) -> BlockStream {
        BlockStream {
            inner,
            rest: Some((context, pool)),
            eof: false,
            block_index: 0,
        }
    }
}

impl Stream for BlockStream {
    type Item = Result<Block>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.eof {
                return Poll::Ready(None);
            }

            let packet = match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err.into()))),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.eof = true;
                    continue;
                }
                Poll::Ready(Some(Ok(packet))) => packet,
            };

            match packet {
                Packet::Eof(inner) => {
                    let (context, pool) = self.rest.take().unwrap();
                    let mut client = ClientHandle {
                        inner: Some(inner),
                        context,
                        pool,
                    };
                    if !client.pool.is_attached() {
                        client.pool.attach();
                    }
                    self.eof = true;
                }
                Packet::ProfileInfo(_) | Packet::Progress(_) => {}
                Packet::Exception(exception) => return Poll::Ready(Some(Err(Error::Server(exception)))),
                Packet::Block(block) => {
                    self.block_index += 1;
                    if self.block_index > 1 && !block.is_empty() {
                        return Poll::Ready(Some(Ok(block)));
                    }
                }
                _ => return Poll::Ready(Some(Err(Error::Driver(DriverError::UnexpectedPacket)))),
            }
        }
    }
}
