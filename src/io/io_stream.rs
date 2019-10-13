use std::{io, time::Duration};

use tokio::{net::TcpStream, prelude::*};
#[cfg(feature = "ssl")]
use tokio_tls::{self, TlsConnector, TlsStream};

#[cfg(feature = "ssl")]
use crate::{
    errors,
    io::BoxFuture,
    types::Options,
};

use crate::errors::Result;

#[derive(Debug)]
pub(crate) enum IoStream {
    Plain(TcpStream),
    #[cfg(feature = "ssl")]
    Secure(TlsStream<TcpStream>),
}

impl IoStream {
    pub fn set_keepalive(&self, duration: Option<Duration>) -> Result<()> {
        match *self {
            IoStream::Plain(ref stream) => stream.set_keepalive(duration)?,
            #[cfg(feature = "ssl")]
            IoStream::Secure(ref stream) => stream.get_ref().get_ref().set_keepalive(duration)?,
        }
        Ok(())
    }

    pub fn set_nodelay(&self, val: bool) -> Result<()> {
        match *self {
            IoStream::Plain(ref stream) => stream.set_nodelay(val)?,
            #[cfg(feature = "ssl")]
            IoStream::Secure(ref stream) => stream.get_ref().get_ref().set_nodelay(val)?,
        }
        Ok(())
    }

    #[cfg(feature = "ssl")]
    pub fn make_secure<S>(self, domain: S, options: &Options) -> BoxFuture<Self>
    where
        S: AsRef<str> + Send + 'static
    {
        let mut builder = native_tls::TlsConnector::builder();
        builder.danger_accept_invalid_certs(options.skip_verify);
        if let Some(certificate) = options.certificate.clone() {
            let native_cert = native_tls::Certificate::from(certificate);
            builder.add_root_certificate(native_cert);
        }

        let connector = builder.build().map(TlsConnector::from);
        let fut = connector.into_future()
            .and_then(move |tls_connector| {
                match self {
                    IoStream::Plain(stream) => {
                        tls_connector.connect(domain.as_ref(), stream)
                    },
                    IoStream::Secure(_) => unreachable!(),
                }
            })
            .map(IoStream::Secure)
            .map_err(|err| errors::Error::Other(err.into()));

        Box::new(fut)
    }
}

impl From<TcpStream> for IoStream {
    fn from(stream: TcpStream) -> Self {
        IoStream::Plain(stream)
    }
}

impl IoStream {
    pub(crate) fn poll_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            IoStream::Plain(ref mut stream) => stream.read(buf),
            #[cfg(feature = "ssl")]
            IoStream::Secure(ref mut stream) => stream.read(buf),
        }
    }

    pub fn poll_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            IoStream::Plain(ref mut stream) => stream.write(buf),
            #[cfg(feature = "ssl")]
            IoStream::Secure(ref mut stream) => stream.write(buf),
        }
    }
}