use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[cfg(feature = "tokio_io")]
use tokio::{io::ReadBuf, net::TcpStream};
#[cfg(feature = "tls")]
use tokio_native_tls::TlsStream;

#[cfg(feature = "async_std")]
use async_std::io::prelude::*;
#[cfg(feature = "async_std")]
use async_std::net::TcpStream;
use pin_project::pin_project;
#[cfg(feature = "tokio_io")]
use tokio::io::{AsyncRead, AsyncWrite};

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
    #[allow(clippy::all)]
    pub(crate) fn set_keepalive(&mut self, keepalive: Option<Duration>) -> io::Result<()> {
        // match *self {
        //     Self::Plain(ref mut stream) => stream.set_keepalive(keepalive),
        //     #[cfg(feature = "tls")]
        //     Self::Secure(ref mut stream) => stream.get_mut().set_keepalive(keepalive),
        // }.map_err(|err| io::Error::new(err.kind(), format!("set_keepalive error: {}", err)))
        if keepalive.is_some() {
            // https://github.com/tokio-rs/tokio/issues/3082
            log::warn!("`tokio` dropped `set_keepalive` in v0.3 and is currently working on getting it back")
        }
        Ok(())
    }

    pub(crate) fn set_nodelay(&mut self, nodelay: bool) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_nodelay(nodelay),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => {
                stream.get_mut().get_mut().get_mut().set_nodelay(nodelay)
            }
        }
        .map_err(|err| io::Error::new(err.kind(), format!("set_nodelay error: {err}")))
    }

    #[cfg(feature = "async_std")]
    pub(crate) fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Plain(stream) => stream.poll_read(cx, buf),
            #[cfg(feature = "tls")]
            StreamProj::Secure(stream) => stream.poll_read(cx, buf),
        }
    }

    #[cfg(not(feature = "async_std"))]
    pub(crate) fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);

        let result = match self.project() {
            StreamProj::Plain(stream) => stream.poll_read(cx, &mut read_buf),
            #[cfg(feature = "tls")]
            StreamProj::Secure(stream) => stream.poll_read(cx, &mut read_buf),
        };

        match result {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(x)) => Poll::Ready(Err(x)),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Plain(stream) => stream.poll_write(cx, buf),
            #[cfg(feature = "tls")]
            StreamProj::Secure(stream) => stream.poll_write(cx, buf),
        }
    }
}
