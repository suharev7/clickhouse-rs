use std::{
    io,
    time::Duration,
};

use tokio::net::TcpStream;
use tokio_tls::TlsStream;

type SecureTcpStream = TlsStream<TcpStream>;

pub(crate) enum Stream {
    Plain(TcpStream),
    Secure(SecureTcpStream),
}

impl From<TcpStream> for Stream {
    fn from(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }
}

impl From<SecureTcpStream> for Stream {
    fn from(stream: SecureTcpStream) -> Stream {
        Self::Secure(stream)
    }
}

impl Stream {
    pub(crate) fn set_nodelay(&mut self, nodelay: bool) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_nodelay(nodelay),
            Self::Secure(ref mut stream) => stream.get_mut().get_mut().set_nodelay(nodelay),
        }
    }

    pub(crate) fn set_keepalive(&mut self, keepalive: Option<Duration>) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.set_keepalive(keepalive),
            Self::Secure(ref mut stream) => stream.get_mut().get_mut().set_keepalive(keepalive),
        }
    }
}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Self::Plain(ref mut stream) => stream.read(buf),
            Self::Secure(ref mut stream) => stream.read(buf),
        }
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Self::Plain(ref mut stream) => stream.write(buf),
            Self::Secure(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            Self::Plain(ref mut stream) => stream.flush(),
            Self::Secure(ref mut stream) => stream.flush(),
        }
    }
}
