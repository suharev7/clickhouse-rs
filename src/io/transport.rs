use std::collections::VecDeque;
use std::io::{self, Cursor};
use std::mem;

use chrono_tz::Tz;
use futures::{Async, Poll, Stream};
use tokio::net::TcpStream;
use tokio::prelude::*;

use crate::binary::Parser;
use crate::io::IoFuture;
use crate::pool::Pool;
use crate::types::{ClickhouseError, Cmd, Context, Packet};
use crate::ClientHandle;

/// Line transport
pub struct ClickhouseTransport {
    // Inner socket
    inner: TcpStream,
    // Set to true when inner.read returns Ok(0);
    done: bool,
    // Buffered read data
    rd: Vec<u8>,
    // Current buffer to write to the socket
    wr: io::Cursor<Vec<u8>>,
    // Queued commands
    cmds: VecDeque<Cmd>,
    // Server time zone
    tz: Option<Tz>,
    compress: bool,
}

enum PacketStreamState {
    Ask,
    Receive,
    Yield(Option<Packet<ClickhouseTransport>>),
    Done,
}

pub struct PacketStream {
    inner: Option<ClickhouseTransport>,
    state: PacketStreamState,
    read_block: bool,
}

impl ClickhouseTransport {
    pub fn new(inner: TcpStream, compress: bool) -> ClickhouseTransport {
        ClickhouseTransport {
            inner,
            done: false,
            rd: vec![],
            wr: io::Cursor::new(vec![]),
            cmds: VecDeque::new(),
            tz: None,
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
                return Err(e);
            }
        }
    }
}

impl ClickhouseTransport {
    fn send(&mut self) -> Poll<(), io::Error> {
        loop {
            if self.wr_is_empty() {
                match self.cmds.pop_front() {
                    None => {
                        return Ok(Async::Ready(()));
                    }
                    Some(cmd) => self.wr = Cursor::new(cmd.get_packed_command()),
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
    type Error = io::Error;

    /// Read a message from the `Transport`
    fn poll(&mut self) -> Poll<Option<Packet<()>>, io::Error> {
        // First fill the buffer
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

                    return Err(e);
                }
            }
        }

        // Try to parse some data!
        let pos;
        let ret = {
            let mut cursor = Cursor::new(&self.rd);
            let res = {
                let mut parser = Parser::new(&mut cursor, self.tz, self.compress);
                parser.parse_packet()
            };
            pos = cursor.position() as usize;

            if let Ok(Packet::Hello(_, ref packet)) = res {
                self.tz = Some(packet.timezone);
            }

            match res {
                Ok(val) => Ok(Async::Ready(Some(val))),
                Err(e) => e.into(),
            }
        };

        match ret {
            Ok(Async::NotReady) => {}
            _ => {
                // Data is consumed
                let tail = self.rd.split_off(pos);
                mem::replace(&mut self.rd, tail);
            }
        }

        ret
    }
}

impl PacketStream {
    pub fn read_block(mut self, context: Context, pool: Option<Pool>) -> IoFuture<ClientHandle> {
        self.read_block = true;

        Box::new(
            self.fold(None, move |acc, package| match package {
                Packet::Eof(inner) => {
                    let client = ClientHandle {
                        inner: Some(inner),
                        context: context.clone(),
                        pool: pool.clone(),
                    };
                    future::ok::<_, std::io::Error>(Some(client))
                }
                Packet::Block(_) => future::ok::<_, std::io::Error>(acc),
                Packet::Exception(e) => future::err(ClickhouseError::Internal(e).into()),
                _ => future::err(ClickhouseError::UnexpectedPacket.into()),
            })
            .map(Option::unwrap),
        )
    }
}

impl Stream for PacketStream {
    type Item = Packet<ClickhouseTransport>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Packet<ClickhouseTransport>>, io::Error> {
        loop {
            self.state = match self.state {
                PacketStreamState::Ask => match self.inner {
                    None => PacketStreamState::Done,
                    Some(ref mut inner) => {
                        let _ = try_ready!(inner.send());
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
                            PacketStreamState::Yield(Some(result))
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
    pub fn call(mut self, req: Cmd) -> PacketStream {
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
