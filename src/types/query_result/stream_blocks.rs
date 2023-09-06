use std::{
    borrow::Cow,
    io::ErrorKind,
    pin::Pin,
    task::{self, Poll},
};

use futures_core::Stream;
use futures_util::StreamExt;

use crate::{
    errors::{DriverError, Error, Result},
    io::transport::PacketStream,
    types::{Block, Packet},
    ClientHandle,
};

pub(crate) struct BlockStream<'a> {
    client: &'a mut ClientHandle,
    inner: PacketStream,
    state: BlockStreamState,
    block_index: usize,
    skip_first_block: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum BlockStreamState {
    /// Currently reading from block packet stream; some further packets may be pending
    Reading,
    /// Completely finished reading from block packet stream; connection is now idle
    Finished,
    /// There was an error reading packet; transport is broken
    Error,
}

impl<'a> Drop for BlockStream<'a> {
    fn drop(&mut self) {
        match self.state {
            BlockStreamState::Reading => {
                if !self.client.pool.is_attached() {
                    self.client.pool.attach();
                }

                if let Some(mut transport) = self.inner.take_transport() {
                    transport.inconsistent = true;
                    self.client.inner = Some(transport);
                }
            }
            BlockStreamState::Finished => {}
            BlockStreamState::Error => {
                // drop broken transport; don't return it to pool to prevent pool poisoning
            }
        }
    }
}

impl<'a> BlockStream<'a> {
    pub(crate) fn new(
        client: &mut ClientHandle,
        inner: PacketStream,
        skip_first_block: bool,
    ) -> BlockStream {
        BlockStream {
            client,
            inner,
            state: BlockStreamState::Reading,
            block_index: 0,
            skip_first_block,
        }
    }
}

impl<'a> Stream for BlockStream<'a> {
    type Item = Result<Block>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                BlockStreamState::Reading => {}
                BlockStreamState::Finished => return Poll::Ready(None),
                BlockStreamState::Error => {
                    return Poll::Ready(Some(Err(Error::Other(Cow::Borrowed(
                        "Attempt to read from broken transport",
                    )))))
                }
            };

            let packet = match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err.into()))),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.state = BlockStreamState::Error;
                    return Poll::Ready(Some(Err(Error::Io(std::io::Error::from(
                        ErrorKind::UnexpectedEof,
                    )))));
                }
                Poll::Ready(Some(Ok(packet))) => packet,
            };

            match packet {
                Packet::Eof(inner) => {
                    self.client.inner = Some(inner);
                    if !self.client.pool.is_attached() {
                        self.client.pool.attach();
                    }
                    self.state = BlockStreamState::Finished;
                }
                Packet::ProfileInfo(_) | Packet::Progress(_) => {}
                Packet::Exception(exception) => {
                    self.state = BlockStreamState::Finished;
                    return Poll::Ready(Some(Err(Error::Server(exception))));
                }
                Packet::Block(block) => {
                    self.block_index += 1;
                    if (self.block_index > 1 || !self.skip_first_block) && !block.is_empty() {
                        return Poll::Ready(Some(Ok(block)));
                    }
                }
                _ => return Poll::Ready(Some(Err(Error::Driver(DriverError::UnexpectedPacket)))),
            }
        }
    }
}
