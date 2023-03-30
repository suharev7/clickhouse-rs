use std::{
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
    eof: bool,
    block_index: usize,
    skip_first_block: bool,
}

impl<'a> Drop for BlockStream<'a> {
    fn drop(&mut self) {
        if !self.eof && !self.client.pool.is_attached() {
            self.client.pool.attach();
        }

        if self.client.inner.is_none() {
            if let Some(mut transport) = self.inner.take_transport() {
                transport.inconsistent = true;
                self.client.inner = Some(transport);
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
            eof: false,
            block_index: 0,
            skip_first_block,
        }
    }
}

impl<'a> Stream for BlockStream<'a> {
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
                    self.client.inner = Some(inner);
                    if !self.client.pool.is_attached() {
                        self.client.pool.attach();
                    }
                    self.eof = true;
                }
                Packet::ProfileInfo(_) | Packet::Progress(_) => {}
                Packet::Exception(exception) => {
                    self.eof = true;
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
