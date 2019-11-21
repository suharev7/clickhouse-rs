use std::{
    collections::VecDeque,
    io::{self, Cursor},
    ptr,
    sync::{
        self,
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use chrono_tz::Tz;
use futures::{Async, Poll, Stream};
use tokio::prelude::*;

use crate::{
    binary::Parser,
    errors::{DriverError, Error},
    io::{BoxFuture, Stream as InnerStream},
    pool::{Inner, PoolBinding},
    types::{Block, Cmd, Context, Packet},
    ClientHandle, Pool,
};

/// Line transport
pub(crate) struct ClickhouseTransport {
    // Inner socket
    inner: InnerStream,
    // Set to true when inner.read returns Ok(0);
    done: bool,
    // Buffered read data
    rd: Vec<u8>,
    // Whether the buffer is known to be incomplete
    buf_is_incomplete: bool,
    // Current buffer to write to the socket
    wr: io::Cursor<Vec<u8>>,
    // Queued commands
    cmds: VecDeque<Cmd>,
    // Server time zone
    timezone: Option<Tz>,
    compress: bool,
    status: Arc<TransportStatus>,
}

enum PacketStreamState {
    Ask,
    Receive,
    Yield(Box<Option<Packet<ClickhouseTransport>>>),
    Done,
}

pub(crate) struct TransportStatus {
    inside: AtomicBool,
    pool: sync::Weak<Inner>,
}

pub(crate) struct PacketStream {
    inner: Option<ClickhouseTransport>,
    state: PacketStreamState,
    read_block: bool,
}

impl ClickhouseTransport {
    pub fn new(inner: InnerStream, compress: bool, pool: Option<Pool>) -> Self {
        ClickhouseTransport {
            inner,
            done: false,
            rd: vec![],
            buf_is_incomplete: false,
            wr: io::Cursor::new(vec![]),
            cmds: VecDeque::new(),
            timezone: None,
            compress,
            status: Arc::new(TransportStatus::new(pool)),
        }
    }

    pub(crate) fn set_inside(&self, value: bool) {
        self.status.inside.store(value, Ordering::Release);
    }
}

impl Drop for TransportStatus {
    fn drop(&mut self) {
        let inside = self.inside.load(Ordering::Acquire);

        if inside {
            return;
        }

        if let Some(pool_inner) = self.pool.upgrade() {
            pool_inner.release_conn();
        }
    }
}

impl TransportStatus {
    fn new(pool: Option<Pool>) -> TransportStatus {
        let pool = match pool {
            None => sync::Weak::new(),
            Some(p) => Arc::downgrade(&p.inner),
        };

        TransportStatus {
            inside: AtomicBool::new(true),
            pool,
        }
    }
}

impl ClickhouseTransport {
    fn wr_is_empty(&self) -> bool {
        self.wr_remaining() == 0
    }

    fn wr_remaining(&self) -> usize {
        self.wr.get_ref().len() - self.wr_pos()
    }

    fn wr_pos(&self) -> usize {
        self.wr.position() as usize
    }

    fn wr_flush(&mut self) -> io::Result<bool> {
        // Making the borrow checker happy
        let res = {
            let buf = {
                let pos = self.wr.position() as usize;
                let buf = &self.wr.get_ref()[pos..];

                trace!("writing; remaining={:?}", buf);
                buf
            };

            self.inner.write(buf)
        };

        match res {
            Ok(mut n) => {
                n += self.wr.position() as usize;
                self.wr.set_position(n as u64);
                Ok(true)
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(false);
                }

                trace!("transport flush error; err={:?}", e);
                Err(e)
            }
        }
    }

    fn try_parse_msg(&mut self) -> Poll<Option<Packet<()>>, Error> {
        let pos;
        let ret = {
            let mut cursor = Cursor::new(&self.rd);
            let res = {
                let mut parser = Parser::new(&mut cursor, self.timezone, self.compress);
                parser.parse_packet()
            };
            pos = cursor.position() as usize;

            if let Ok(Packet::Hello(_, ref packet)) = res {
                self.timezone = Some(packet.timezone);
            }

            match res {
                Ok(val) => Ok(Async::Ready(Some(val))),
                Err(e) => e.into(),
            }
        };

        match ret {
            Ok(Async::NotReady) => (),
            _ => {
                // Data is consumed
                let new_len = self.rd.len() - pos;
                unsafe {
                    ptr::copy(self.rd.as_ptr().add(pos), self.rd.as_mut_ptr(), new_len);
                    self.rd.set_len(new_len);
                }
            }
        }

        ret
    }
}

impl ClickhouseTransport {
    fn send(&mut self) -> Poll<(), Error> {
        loop {
            if self.wr_is_empty() {
                match self.cmds.pop_front() {
                    None => {
                        return Ok(Async::Ready(()));
                    }
                    Some(cmd) => {
                        let bytes = cmd.get_packed_command()?;
                        self.wr = Cursor::new(bytes)
                    }
                }
            }

            // Try to write the remaining buffer
            if !self.wr_flush()? {
                return Ok(Async::NotReady);
            }
        }
    }
}

impl Stream for ClickhouseTransport {
    type Item = Packet<()>;
    type Error = Error;

    /// Read a message from the `Transport`
    fn poll(&mut self) -> Poll<Option<Packet<()>>, Error> {
        // Check whether our currently buffered data is enough for a packet
        // before reading any more data. This prevents the buffer from growing
        // indefinitely when the sender is faster than we can consume the data
        if !self.buf_is_incomplete && !self.rd.is_empty() {
            if let ret @ Async::Ready(_) = self.try_parse_msg()? {
                return Ok(ret);
            }
        }

        // Fill the buffer!
        while !self.done {
            match self.inner.read_to_end(&mut self.rd) {
                Ok(0) => {
                    self.done = true;
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        break;
                    }

                    return Err(e.into());
                }
            }
        }

        // Try to parse the new data!
        let ret = self.try_parse_msg();

        self.buf_is_incomplete = if let Ok(Async::NotReady) = ret { true } else { false };

        ret
    }
}

impl PacketStream {
    pub(crate) fn read_block(
        mut self,
        context: Context,
        pool: PoolBinding,
    ) -> BoxFuture<(ClientHandle, Option<Block>)> {
        self.read_block = true;

        Box::new(
            self.fold((None, None), move |(c, b), package| match package {
                Packet::Eof(inner) => {
                    let client = ClientHandle {
                        inner: Some(inner),
                        context: context.clone(),
                        pool: pool.clone(),
                    };
                    future::ok::<_, Error>((Some(client), b))
                }
                Packet::Block(block) => future::ok::<_, Error>((c, Some(block))),
                Packet::Exception(e) => future::err(Error::Server(e)),
                _ => future::err(Error::Driver(DriverError::UnexpectedPacket)),
            })
            .map(|(c, b)| (c.unwrap(), b)),
        )
    }
}

impl Stream for PacketStream {
    type Item = Packet<ClickhouseTransport>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Packet<ClickhouseTransport>>, Self::Error> {
        loop {
            self.state = match self.state {
                PacketStreamState::Ask => match self.inner {
                    None => PacketStreamState::Done,
                    Some(ref mut inner) => {
                        try_ready!(inner.send());
                        PacketStreamState::Receive
                    }
                },
                PacketStreamState::Receive => {
                    let ret = match self.inner {
                        None => None,
                        Some(ref mut inner) => try_ready!(inner.poll()),
                    };

                    match ret {
                        None => PacketStreamState::Done,
                        Some(packet) => {
                            let result = packet.bind(&mut self.inner);
                            PacketStreamState::Yield(Box::new(Some(result)))
                        }
                    }
                }
                PacketStreamState::Yield(_) => PacketStreamState::Receive,
                PacketStreamState::Done => {
                    return match self.inner.take() {
                        Some(inner) => Ok(Async::Ready(Some(Packet::Eof(inner)))),
                        _ => Ok(Async::Ready(None)),
                    };
                }
            };

            let package = match self.state {
                PacketStreamState::Yield(ref mut packet) => packet.take(),
                _ => None,
            };

            if self.read_block && is_block(&package) {
                self.state = PacketStreamState::Done;
            }

            if package.is_some() {
                return Ok(Async::Ready(package));
            }
        }
    }
}

impl ClickhouseTransport {
    pub(crate) fn call(mut self, req: Cmd) -> PacketStream {
        self.cmds.push_back(req);
        PacketStream {
            inner: Some(self),
            state: PacketStreamState::Ask,
            read_block: false,
        }
    }
}

fn is_block<T>(packet: &Option<Packet<T>>) -> bool {
    match packet {
        Some(Packet::Block(_)) => true,
        _ => false,
    }
}
