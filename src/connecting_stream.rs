use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::future::{select_ok, BoxFuture, SelectOk, TryFutureExt};
#[cfg(feature = "_tls")]
use futures_util::FutureExt;

#[cfg(feature = "async_std")]
use async_std::net::TcpStream;
#[cfg(feature = "tls-native-tls")]
use native_tls::TlsConnector;
#[cfg(feature = "tokio_io")]
use tokio::net::TcpStream;
#[cfg(feature = "tls-rustls")]
use {
    rustls::{
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        crypto::{verify_tls12_signature, verify_tls13_signature},
        pki_types::{CertificateDer, ServerName, UnixTime},
        ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore,
    },
    std::sync::Arc,
    tokio_rustls::TlsConnector,
};

use pin_project::pin_project;
use url::Url;

use crate::{errors::ConnectionError, io::Stream as InnerStream, Options};
#[cfg(feature = "tls-native-tls")]
use tokio_native_tls::TlsStream;
#[cfg(feature = "tls-rustls")]
use tokio_rustls::client::TlsStream;

type Result<T> = std::result::Result<T, ConnectionError>;

type ConnectingFuture<T> = BoxFuture<'static, Result<T>>;

#[pin_project(project = TcpStateProj)]
enum TcpState {
    Wait(#[pin] SelectOk<ConnectingFuture<TcpStream>>),
    Fail(Option<ConnectionError>),
}

#[cfg(feature = "_tls")]
#[pin_project(project = TlsStateProj)]
enum TlsState {
    Wait(#[pin] ConnectingFuture<TlsStream<TcpStream>>),
    Fail(Option<ConnectionError>),
}

#[pin_project(project = StateProj)]
enum State {
    Tcp(#[pin] TcpState),
    #[cfg(feature = "_tls")]
    Tls(#[pin] TlsState),
}

impl TcpState {
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<InnerStream>> {
        match self.project() {
            TcpStateProj::Wait(inner) => match inner.poll(cx) {
                Poll::Ready(Ok((tcp, _))) => Poll::Ready(Ok(InnerStream::Plain(tcp))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            TcpStateProj::Fail(ref mut err) => Poll::Ready(Err(err.take().unwrap())),
        }
    }
}

#[cfg(feature = "_tls")]
impl TlsState {
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<InnerStream>> {
        match self.project() {
            TlsStateProj::Wait(ref mut inner) => match inner.poll_unpin(cx) {
                Poll::Ready(Ok(tls)) => Poll::Ready(Ok(InnerStream::Secure(tls))),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            TlsStateProj::Fail(ref mut err) => {
                let e = err.take().unwrap();
                Poll::Ready(Err(e))
            }
        }
    }
}

impl State {
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<InnerStream>> {
        match self.project() {
            StateProj::Tcp(inner) => inner.poll(cx),
            #[cfg(feature = "_tls")]
            StateProj::Tls(inner) => inner.poll(cx),
        }
    }

    fn tcp_err(e: io::Error) -> Self {
        let conn_error = ConnectionError::IoError(e);
        State::Tcp(TcpState::Fail(Some(conn_error)))
    }

    #[cfg(feature = "_tls")]
    fn tls_host_err() -> Self {
        State::Tls(TlsState::Fail(Some(ConnectionError::TlsHostNotProvided)))
    }

    fn tcp_wait(socket: SelectOk<ConnectingFuture<TcpStream>>) -> Self {
        State::Tcp(TcpState::Wait(socket))
    }

    #[cfg(feature = "_tls")]
    fn tls_wait(s: ConnectingFuture<TlsStream<TcpStream>>) -> Self {
        State::Tls(TlsState::Wait(s))
    }
}

#[pin_project]
pub(crate) struct ConnectingStream {
    #[pin]
    state: State,
}

#[derive(Debug)]
struct DummyTlsVerifier;

#[cfg(feature = "tls-rustls")]
impl ServerCertVerifier for DummyTlsVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
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

                #[cfg(feature = "_tls")]
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

    #[cfg(feature = "tls-native-tls")]
    fn new_tls_connection(
        addr: &Url,
        socket: SelectOk<ConnectingFuture<TcpStream>>,
        options: &Options,
    ) -> Self {
        match addr.host_str().map(|host| host.to_owned()) {
            None => Self {
                state: State::tls_host_err(),
            },
            Some(host) => {
                let mut builder = TlsConnector::builder();
                builder.danger_accept_invalid_certs(options.skip_verify);
                if let Some(certificate) = options.certificate.clone() {
                    let native_cert = native_tls::Certificate::from(certificate);
                    builder.add_root_certificate(native_cert);
                }

                Self {
                    state: State::tls_wait(Box::pin(async move {
                        let (s, _) = socket.await?;

                        let cx = builder.build()?;
                        let cx = tokio_native_tls::TlsConnector::from(cx);

                        Ok(cx.connect(&host, s).await?)
                    })),
                }
            }
        }
    }

    #[cfg(feature = "tls-rustls")]
    fn new_tls_connection(
        addr: &Url,
        socket: SelectOk<ConnectingFuture<TcpStream>>,
        options: &Options,
    ) -> Self {
        match addr.host_str().map(|host| host.to_owned()) {
            None => Self {
                state: State::tls_host_err(),
            },
            Some(host) => {
                let config = if options.skip_verify {
                    ClientConfig::builder()
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(DummyTlsVerifier))
                        .with_no_client_auth()
                } else {
                    let mut cert_store = RootCertStore::empty();
                    cert_store.extend(
                        webpki_roots::TLS_SERVER_ROOTS
                            .iter()
                            .cloned()
                    );
                    if let Some(certificates) = options.certificate.clone() {
                        for certificate in
                            Into::<Vec<rustls::pki_types::CertificateDer<'static>>>::into(
                                certificates,
                            )
                        {
                            match cert_store.add(certificate) {
                                Ok(_) => {},
                                Err(err) => {
                                    let err = io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("Could not load certificate: {}.", err),
                                    );
                                    return Self { state: State::tcp_err(err) };
                                },
                            }
                        }
                    }
                    ClientConfig::builder()
                        .with_root_certificates(cert_store)
                        .with_no_client_auth()
                };
                Self {
                    state: State::tls_wait(Box::pin(async move {
                        let (s, _) = socket.await?;
                        let cx = TlsConnector::from(Arc::new(config));
                        let host = ServerName::try_from(host)
                            .map_err(|_| ConnectionError::TlsHostNotProvided)?;
                        Ok(cx
                            .connect(host, s)
                            .await
                            .map_err(|e| ConnectionError::IoError(e))?)
                    })),
                }
            }
        }
    }
}

impl Future for ConnectingStream {
    type Output = Result<InnerStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().state.poll(cx)
    }
}
