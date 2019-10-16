use std::io;

use futures::{future::FutureResult, SelectOk};
use tokio::net::{tcp::ConnectFuture, TcpStream};
use tokio::prelude::*;
use url::Url;

#[cfg(feature = "tls")]
use native_tls::TlsConnector;
#[cfg(feature = "tls")]
use tokio_tls::TlsStream;

use crate::{
    errors::ConnectionError,
    io::Stream as InnerStream,
    types::Options,
};

#[cfg(feature = "tls")]
type ConnectingFuture<T> = Box<dyn Future<Item = T, Error = ConnectionError> + Send>;

impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}

#[cfg(feature = "tls")]
impl From<native_tls::Error> for ConnectionError {
    fn from(e: native_tls::Error) -> Self {
        Self::TlsError(e)
    }
}

enum TcpState {
    Wait(SelectOk<ConnectFuture>),
    Fail(FutureResult<TcpStream, ConnectionError>),
}

#[cfg(feature = "tls")]
enum TlsState {
    Wait(ConnectingFuture<TlsStream<TcpStream>>),
    Fail(FutureResult<TlsStream<TcpStream>, ConnectionError>),
}

enum State {
    Tcp(TcpState),
    #[cfg(feature = "tls")]
    Tls(TlsState),
}

impl State {
    fn poll(&mut self) -> Poll<InnerStream, ConnectionError> {
        match self {
            Self::Tcp(state) => {
                match state {
                    TcpState::Wait(ref mut inner) => match inner.poll() {
                        Ok(Async::Ready((tcp, _))) => Ok(Async::Ready(tcp.into())),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Err(err) => Err(err.into()),
                    },
                    TcpState::Fail(ref mut inner) => match inner.poll() {
                        Err(err) => Err(err),
                        _ => unreachable!(),
                    },
                }
            },
            #[cfg(feature = "tls")]
            Self::Tls(state) => {
                match state {
                    TlsState::Wait(ref mut inner) => match inner.poll() {
                        Ok(Async::Ready(tls)) => Ok(Async::Ready(tls.into())),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Err(err) => Err(err),
                    },
                    TlsState::Fail(ref mut inner) => match inner.poll() {
                        Err(err) => Err(err),
                        _ => unreachable!(),
                    },
                }
            },
        }
    }

    fn tcp_err(e: io::Error) -> Self {
        Self::Tcp(TcpState::Fail(future::err(e.into())))
    }

    #[cfg(feature = "tls")]
    fn tls_host_err() -> Self {
        Self::Tls(TlsState::Fail(future::err(ConnectionError::TlsHostNotProvided)))
    }

    fn tcp_wait(s: SelectOk<ConnectFuture>) -> Self {
        Self::Tcp(TcpState::Wait(s))
    }

    #[cfg(feature = "tls")]
    fn tls_wait(s: ConnectingFuture<TlsStream<TcpStream>>) -> Self {
        Self::Tls(TlsState::Wait(s))
    }
}

pub(crate) struct ConnectingStream {
    state: State,
}

#[cfg(feature = "tls")]
impl ConnectingStream {
    pub(crate) fn new(addr: &Url, options: &Options) -> Self
    where
    {
        match addr.socket_addrs(|| None) {
            Ok(addresses) => {
                let streams: Vec<_> = addresses
                    .iter()
                    .map(|address| TcpStream::connect(address))
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

                let socket = future::select_ok(streams);

                if !options.secure {
                    return Self {
                        state: State::tcp_wait(socket),
                    };
                }

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
                                Box::new(
                                    socket
                                        .from_err::<ConnectionError>()
                                        .join(builder.build().into_future().from_err())
                                        .and_then(move |((s, _), cx)| {
                                            let cx = tokio_tls::TlsConnector::from(cx);

                                            cx.connect(&host, s).from_err()
                                        }))
                            )
                        }
                    },
                }
            }
            Err(err) => Self {
                state: State::tcp_err(err),
            },
        }
    }
}

#[cfg(not(feature = "tls"))]
impl ConnectingStream {
    pub(crate) fn new(addr: &Url, _options: &Options) -> Self
        where
    {
        match addr.socket_addrs(|| None) {
            Ok(addresses) => {
                let streams: Vec<_> = addresses
                    .iter()
                    .map(|address| TcpStream::connect(address))
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

                let socket = future::select_ok(streams);

                return Self {
                    state: State::tcp_wait(socket),
                };
            }
            Err(err) => Self {
                state: State::tcp_err(err),
            },
        }
    }
}

impl Future for ConnectingStream {
    type Item = InnerStream;
    type Error = ConnectionError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.state.poll()
    }
}
