use futures::{Async, Poll, Stream};

use crate::{
    errors::{DriverError, Error},
    io::transport::PacketStream,
    pool::PoolBinding,
    types::{Block, Context, Packet, query_result::set_exception_handle},
    ClientHandle,
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
    type Item = Block;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            if self.eof {
                return Ok(Async::Ready(None));
            }

            let packet = match self.inner.poll() {
                Err(err) => return Err(err),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => {
                    self.eof = true;
                    continue;
                }
                Ok(Async::Ready(Some(packet))) => packet,
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
                Packet::Exception(mut exception, transport) => {
                    let (context, pool) = self.rest.take().unwrap();
                    set_exception_handle(&mut exception, transport, context, pool);
                    return Err(Error::Server(exception))
                },
                Packet::Block(block) => {
                    self.block_index += 1;
                    if self.block_index > 1 && !block.is_empty() {
                        return Ok(Async::Ready(Some(block)));
                    }
                }
                _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
            }
        }
    }
}
