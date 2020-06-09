use std::{
    io,
    time::Duration,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "tokio_io")]
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio_tls::TlsStream;

#[cfg(feature = "tokio_io")]
use tokio::prelude::*;
use pin_project::pin_project;
#[cfg(feature = "async_std")]
use async_std::net::TcpStream;
#[cfg(feature = "async_std")]
use async_std::io::prelude::*;

#[cfg(all(feature = "tls", feature = "tokio_io"))]
type SecureTcpStream = TlsStream<TcpStream>;

#[pin_project(project = StreamProj)]
pub(crate) enum Stream {
    Plain(#[pin] TcpStream),
    #[cfg(feature = "tls")]
    Secure(#[pin] SecureTcpStream),
}

impl From<TcpStream> for Stream {
    fn from(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }
}

#[cfg(feature = "tls")]
impl From<SecureTcpStream> for Stream {
    fn from(stream: SecureTcpStream) -> Stream {
        Self::Secure(stream)
    }
}

impl Stream {
    #[cfg(feature = "async_std")]
    pub(crate) fn set_keepalive(&mut self, keepalive: Option<Duration>) -> io::Result<()> {
        if keepalive.is_some() {
            log::warn!("`std_async` doesn't support `set_keepalive`.")
        }
        Ok(())
    }

    #[cfg(not(feature = "async_std"))]
    pub(crate) fn set_keepalive(&mut self, keepalive: Option<Duration>) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_keepalive(keepalive),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => stream.get_mut().set_keepalive(keepalive),
        }.map_err(|err| io::Error::new(err.kind(), format!("set_keepalive error: {}", err)))
    }

    pub(crate) fn set_nodelay(&mut self, nodelay: bool) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_nodelay(nodelay),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => stream.get_mut().set_nodelay(nodelay),
        }.map_err(|err| io::Error::new(err.kind(), format!("set_nodelay error: {}", err)))
    }

    pub(crate) fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Plain(stream) => stream.poll_read(cx, buf),
            #[cfg(feature = "tls")]
            StreamProj::Secure(stream) => stream.poll_read(cx, buf),
        }
    }

    pub(crate) fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Plain(stream) => stream.poll_write(cx, buf),
            #[cfg(feature = "tls")]
            StreamProj::Secure(stream) => stream.poll_write(cx, buf),
        }
    }
}
