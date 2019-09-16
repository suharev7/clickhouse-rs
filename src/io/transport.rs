use std::{
    collections::VecDeque,
    io::{self, Cursor},
    mem,
    pin::Pin,
    task::{self, Poll},
};

use chrono_tz::Tz;
use log::trace;
use tokio::{net::TcpStream, prelude::*};

use pin_project::pin_project;

use crate::{
    binary::Parser,
    ClientHandle,
    errors::{DriverError, Error, Result},
    io::read_to_end::read_to_end,
    pool::PoolBinding,
    types::{self, Block, Cmd, Packet}
};

/// Line transport
#[pin_project]
pub(crate) struct ClickhouseTransport {
    // Inner socket
    #[pin]
    inner: TcpStream,
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
}

enum PacketStreamState {
    Ask,
    Receive,
    Yield(Box<Option<Packet<ClickhouseTransport>>>),
    Done,
}

pub(crate) struct PacketStream {
    inner: Option<ClickhouseTransport>,
    state: PacketStreamState,
    read_block: bool,
}

impl ClickhouseTransport {
    pub fn new(inner: TcpStream, compress: bool) -> Self {
        ClickhouseTransport {
            inner,
            done: false,
            rd: vec![],
            buf_is_incomplete: false,
            wr: io::Cursor::new(vec![]),
            cmds: VecDeque::new(),
            timezone: None,
            compress,
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

    fn wr_flush(&mut self, cx: &mut task::Context) -> io::Result<bool> {
        // Making the borrow checker happy
        let res = {
            let buf = {
                let pos = self.wr.position() as usize;
                let buf = &self.wr.get_ref()[pos..];

                trace!("writing; remaining={:?}", buf);
                buf
            };

            Pin::new(&mut self.inner).poll_write(cx, buf)
        };

        match res {
            Poll::Ready(Ok(mut n)) => {
                n += self.wr.position() as usize;
                self.wr.set_position(n as u64);
                Ok(true)
            }
            Poll::Ready(Err(e)) => {
                trace!("transport flush error; err={:?}", e);
                Err(e)
            }
            Poll::Pending => Ok(false),
        }
    }

    fn try_parse_msg(mut self: Pin<&mut Self>) -> Poll<Option<io::Result<Packet<()>>>> {
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
                Ok(val) => Poll::Ready(Some(Ok(val))),
                Err(e) => {
                    if e.is_would_block() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(Err(e.into())))
                    }
                },
            }
        };

        match ret {
            Poll::Pending => (),
            _ => {
                // Data is consumed
                let tail = self.rd.split_off(pos);
                mem::replace(&mut self.rd, tail);
            }
        }

        ret
    }
}

impl ClickhouseTransport {
    fn send(&mut self, cx: &mut task::Context) -> Poll<Result<()>> {
        loop {
            if self.wr_is_empty() {
                match self.cmds.pop_front() {
                    None => return Poll::Ready(Ok(())),
                    Some(cmd) => {
                        let bytes = cmd.get_packed_command()?;
                        self.wr = Cursor::new(bytes)
                    }
                }
            }

            // Try to write the remaining buffer
            if !self.wr_flush(cx)? {
                return Poll::Pending;
            }
        }
    }
}

impl Stream for ClickhouseTransport {
    type Item = io::Result<Packet<()>>;

    /// Read a message from the `Transport`
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        // Check whether our currently buffered data is enough for a packet
        // before reading any more data. This prevents the buffer from growing
        // indefinitely when the sender is faster than we can consume the data
        if !self.buf_is_incomplete && !self.rd.is_empty() {
            if let Poll::Ready(ret) = self.as_mut().try_parse_msg()? {
                // let i: Poll<Option<io::Result<Packet<()>>>> = ret;
                return Poll::Ready(ret.map(Ok));
            }
        }

        // Fill the buffer!
        while !self.done {
            let mut this = self.project();
            match read_to_end(this.inner, cx, &mut this.rd) {
                Poll::Ready(Ok(0)) => {
                    self.done = true;
                    break;
                }
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => break,
            }
        }

        // Try to parse the new data!
        let ret = self.as_mut().try_parse_msg();

        self.buf_is_incomplete = if let Poll::Pending = ret {
            true
        } else {
            false
        };

        ret
    }
}

impl PacketStream {
    pub(crate) async fn read_block(
        mut self,
        context: types::Context,
        pool: PoolBinding,
    ) -> Result<(ClientHandle, Option<Block>)> {
        self.read_block = true;

        let mut h = None;
        let mut b = None;
        while let Some(package) = self.next().await {
            match package {
                Ok(Packet::Eof(inner)) => {
                    let client = ClientHandle {
                        inner: Some(inner),
                        context: context.clone(),
                        pool: pool.clone(),
                    };
                    h = Some(client)
                }
                Ok(Packet::Block(block)) => b = Some(block),
                Ok(Packet::Exception(e)) => return Err(Error::Server(e)),
                Err(e) => return Err(e.into()),
                _ => return Err(Error::Driver(DriverError::UnexpectedPacket)),
            }
        }

        Ok((h.unwrap(), b))
    }
}

impl Stream for PacketStream {
    type Item = io::Result<Packet<ClickhouseTransport>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        loop {
            self.state = match self.state {
                PacketStreamState::Ask => match self.inner {
                    None => PacketStreamState::Done,
                    Some(ref mut inner) => {
                        match inner.send(cx) {
                            Poll::Ready(Ok(t)) => t,
                            Poll::Ready(Err(e)) => {
                                if e.is_would_block() {
                                    return Poll::Pending;
                                }

                                return Poll::Ready(Some(Err(e.into())));
                            }
                            Poll::Pending => return Poll::Pending,
                        };
                        PacketStreamState::Receive
                    }
                },
                PacketStreamState::Receive => {
                    let ret = match self.inner {
                        None => None,
                        Some(ref mut inner) => match Pin::new(inner).poll_next(cx) {
                            Poll::Ready(Some(Ok(r))) => Some(r),
                            Poll::Ready(Some(Err(e))) => {
                                if e.kind() == io::ErrorKind::WouldBlock {
                                    return Poll::Pending;
                                }

                                return Poll::Ready(Some(Err(e)));
                            }
                            Poll::Ready(None) => return Poll::Ready(None),
                            Poll::Pending => return Poll::Pending,
                        },
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
                        Some(inner) => Poll::Ready(Some(Ok(Packet::Eof(inner)))),
                        _ => Poll::Ready(None),
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

            if let Some(pkg) = package {
                return Poll::Ready(Some(Ok(pkg)));
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
