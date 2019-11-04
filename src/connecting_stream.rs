use std::{
    future::Future,
    io,
    net::ToSocketAddrs,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::future::BoxFuture;
use futures_util::{
    try_future::{select_ok, SelectOk},
    FutureExt,
};
use tokio::net::TcpStream;

use pin_project::{pin_project, project};

#[pin_project]
enum State {
    Wait(#[pin] SelectOk<BoxFuture<'static, io::Result<TcpStream>>>),
    Fail(Option<io::Error>),
}

impl State {
    #[project]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<TcpStream>> {
        #[project]
        match self.project() {
            State::Wait(ref mut inner) => match inner.poll_unpin(cx) {
                Poll::Ready(Ok((tcp, _))) => Poll::Ready(Ok(tcp)),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            State::Fail(ref mut err) => Poll::Ready(Err(err.take().unwrap())),
        }
    }
}

#[pin_project]
pub(crate) struct ConnectingStream {
    #[pin]
    state: State,
}

impl ConnectingStream {
    pub(crate) fn new<S>(addr: S) -> Self
    where
        S: ToSocketAddrs,
    {
        match addr.to_socket_addrs() {
            Ok(addresses) => {
                let streams: Vec<_> = addresses
                    .map(|address| -> BoxFuture<'static, io::Result<TcpStream>> {
                        Box::pin(TcpStream::connect(address))
                    })
                    .collect();

                if streams.is_empty() {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Could not resolve to any address.",
                    );
                    Self {
                        state: State::Fail(Some(err)),
                    }
                } else {
                    Self {
                        state: State::Wait(select_ok(streams)),
                    }
                }
            }
            Err(err) => Self {
                state: State::Fail(Some(err)),
            },
        }
    }
}

impl Future for ConnectingStream {
    type Output = io::Result<TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().state.poll(cx)
    }
}
