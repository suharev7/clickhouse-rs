use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::future::BoxFuture;
#[cfg(feature = "tls")]
use futures_util::FutureExt;
use futures_util::{try_future::{select_ok, SelectOk}, TryFutureExt};

#[cfg(feature = "tls")]
use native_tls::TlsConnector;
use tokio::net::TcpStream;

use pin_project::{pin_project, project};
use url::Url;

use crate::{errors::ConnectionError, io::Stream as InnerStream, Options};
#[cfg(feature = "tls")]
use tokio_tls::TlsStream;

type Result<T> = std::result::Result<T, ConnectionError>;

type ConnectingFuture<T> = BoxFuture<'static, Result<T>>;

#[pin_project]
enum TcpState {
    Wait(#[pin] SelectOk<ConnectingFuture<TcpStream>>),
    Fail(Option<ConnectionError>),
}

#[cfg(feature = "tls")]
#[pin_project]
enum TlsState {
    Wait(#[pin] ConnectingFuture<TlsStream<TcpStream>>),
    Fail(Option<ConnectionError>),
}

#[pin_project]
enum State {
    Tcp(#[pin] TcpState),
    #[cfg(feature = "tls")]
    Tls(#[pin] TlsState),
}

impl TcpState {
    #[project]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<InnerStream>> {
        #[project]
        match self.project() {
            TcpState::Wait(inner) => match inner.poll(cx) {
                Poll::Ready(Ok((tcp, _))) => Poll::Ready(Ok(InnerStream::Plain(tcp))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            TcpState::Fail(ref mut err) => {
                Poll::Ready(Err(err.take().unwrap()))
            },
        }
    }
}

#[cfg(feature = "tls")]
impl TlsState {
    #[project]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<InnerStream>> {
        #[project]
        match self.project() {
            TlsState::Wait(ref mut inner) => match inner.poll_unpin(cx) {
                Poll::Ready(Ok(tls)) => Poll::Ready(Ok(InnerStream::Secure(tls))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            TlsState::Fail(ref mut err) => {
                let e = err.take().unwrap();
                Poll::Ready(Err(e))
            },
        }
    }
}

impl State {
    #[project]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<InnerStream>> {
        #[project]
        match self.project() {
            State::Tcp(inner) => inner.poll(cx),
            #[cfg(feature = "tls")]
            State::Tls(inner) => inner.poll(cx),
        }
    }

    fn tcp_err(e: io::Error) -> Self {
        let conn_error = ConnectionError::IoError(e);
        State::Tcp(TcpState::Fail(Some(conn_error)))
    }

    #[cfg(feature = "tls")]
    fn tls_host_err() -> Self {
        State::Tls(TlsState::Fail(Some(ConnectionError::TlsHostNotProvided)))
    }

    fn tcp_wait(socket: SelectOk<ConnectingFuture<TcpStream>>) -> Self {
        State::Tcp(TcpState::Wait(socket))
    }

    #[cfg(feature = "tls")]
    fn tls_wait(s: ConnectingFuture<TlsStream<TcpStream>>) -> Self {
        State::Tls(TlsState::Wait(s))
    }
}

#[pin_project]
pub(crate) struct ConnectingStream {
    #[pin]
    state: State,
}

impl ConnectingStream {
    #[allow(unused_variables)]
    pub(crate) fn new(addr: &Url, options: &Options) -> Self {
        match addr.socket_addrs(|| None) {
            Ok(addresses) => {
                let streams: Vec<_> = addresses
                    .iter()
                    .copied()
                    .map(|address| -> ConnectingFuture<TcpStream> {
                        Box::pin(TcpStream::connect(address).map_err(ConnectionError::IoError))
                    })
                    .collect();

                if streams.is_empty() {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Could not resolve to any address.",
                    );
                    return Self {
                        state: State::tcp_err(err),
                    };
                }

                let socket = select_ok(streams);

                #[cfg(feature = "tls")]
                {
                    if options.secure {
                        return ConnectingStream::new_tls_connection(addr, socket, options);
                    }
                }

                Self {
                    state: State::tcp_wait(socket),
                }
            }
            Err(err) => Self {
                state: State::tcp_err(err),
            },
        }
    }

    #[cfg(feature = "tls")]
    fn new_tls_connection(addr: &Url, socket: SelectOk<ConnectingFuture<TcpStream>>, options: &Options) -> Self {
        match addr.host_str().map(|host| host.to_owned()) {
            None => {
                Self {
                    state: State::tls_host_err(),
                }
            },
            Some(host) => {
                let mut builder = TlsConnector::builder();
                builder.danger_accept_invalid_certs(options.skip_verify);
                if let Some(certificate) = options.certificate.clone() {
                    let native_cert = native_tls::Certificate::from(certificate);
                    builder.add_root_certificate(native_cert);
                }

                Self {
                    state: State::tls_wait(
                        Box::pin(async move {
                            let (s, _) = socket.await?;

                            let cx = builder.build()?;
                            let cx = tokio_tls::TlsConnector::from(cx);

                            Ok(cx.connect(&host, s).await?)
                        })
                    )
                }
            },
        }
    }
}

impl Future for ConnectingStream {
    type Output = Result<InnerStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().state.poll(cx)
    }
}
