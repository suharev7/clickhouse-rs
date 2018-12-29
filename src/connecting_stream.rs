use std::{io, net::ToSocketAddrs};

use futures::{future::FutureResult, SelectOk};
use tokio::net::{tcp::ConnectFuture, TcpStream};
use tokio::prelude::*;

enum State {
    Wait(SelectOk<ConnectFuture>),
    Fail(FutureResult<TcpStream, io::Error>),
}

impl State {
    fn poll(&mut self) -> Poll<TcpStream, io::Error> {
        match self {
            State::Wait(ref mut inner) => match inner.poll() {
                Ok(Async::Ready((tcp, _))) => Ok(Async::Ready(tcp)),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(err) => Err(err),
            },
            State::Fail(ref mut inner) => match inner.poll() {
                Err(err) => Err(err),
                _ => unreachable!(),
            },
        }
    }
}

pub(crate) struct ConnectingStream {
    state: State,
}

impl ConnectingStream {
    pub(crate) fn new<S>(addr: S) -> ConnectingStream
    where
        S: ToSocketAddrs,
    {
        match addr.to_socket_addrs() {
            Ok(addresses) => {
                let streams: Vec<_> = addresses
                    .map(|address| TcpStream::connect(&address))
                    .collect();

                if streams.is_empty() {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Could not resolve to any address.",
                    );
                    ConnectingStream {
                        state: State::Fail(future::err(err)),
                    }
                } else {
                    ConnectingStream {
                        state: State::Wait(future::select_ok(streams)),
                    }
                }
            }
            Err(err) => ConnectingStream {
                state: State::Fail(future::err(err)),
            },
        }
    }
}

impl Future for ConnectingStream {
    type Item = TcpStream;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.state.poll()
    }
}
