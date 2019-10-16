use std::{
    io,
    time::Duration,
};

use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio_tls::TlsStream;

#[cfg(feature = "tls")]
type SecureTcpStream = TlsStream<TcpStream>;

pub(crate) enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Secure(SecureTcpStream),
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
    pub(crate) fn set_nodelay(&mut self, nodelay: bool) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_nodelay(nodelay),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => stream.get_mut().get_mut().set_nodelay(nodelay),
        }
    }

    pub(crate) fn set_keepalive(&mut self, keepalive: Option<Duration>) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_keepalive(keepalive),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => stream.get_mut().get_mut().set_keepalive(keepalive),
        }
    }
}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Self::Plain(ref mut stream) => stream.read(buf),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => stream.read(buf),
        }
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Self::Plain(ref mut stream) => stream.write(buf),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.flush(),
            #[cfg(feature = "tls")]
            Self::Secure(ref mut stream) => stream.flush(),
        }
    }
}
