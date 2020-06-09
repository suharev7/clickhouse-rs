use std::{borrow::Cow, io, mem, result, str::Utf8Error, string::FromUtf8Error, sync::Arc};

use thiserror::Error;
use tokio::prelude::*;
use tokio_timer::{timeout::Error as TimeoutError, Error as TimerError};
use url::ParseError;

use crate::{types::Packet, ClientHandle};

/// Clickhouse error codes
pub mod codes;

/// Result type alias for this library.
pub type Result<T> = result::Result<T, Error>;

/// This type enumerates library errors.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Driver error: `{}`", _0)]
    Driver(#[source] DriverError),

    #[error("Input/output error: `{}`", _0)]
    Io(#[source] io::Error),

    #[error("Connections error: `{}`", _0)]
    Connection(#[source] ConnectionError),

    #[error("Other error: `{}`", _0)]
    Other(Cow<'static, str>),

    #[error("Server error: `{}`", _0)]
    Server(#[source] ServerError),

    #[error("URL error: `{}`", _0)]
    Url(#[source] UrlError),

    #[error("From SQL error: `{}`", _0)]
    FromSql(#[source] FromSqlError),
}

/// This type represents Clickhouse server error.
#[derive(Debug, Error, Clone)]
#[error("ERROR {} ({}): {}", name, code, message)]
pub struct ServerError {
    pub code: u32,
    pub name: String,
    pub message: String,
    pub stack_trace: String,
    pub handle: Option<Arc<ClientHandle>>,
}

/// This type enumerates connection errors.
#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("TLS connection requires hostname to be provided")]
    TlsHostNotProvided,

    #[error("Input/output error: `{}`", _0)]
    IoError(#[source] io::Error),

    #[cfg(feature = "tls")]
    #[error("TLS connection error: `{}`", _0)]
    TlsError(#[source] native_tls::Error),
}

/// This type enumerates connection URL errors.
#[derive(Debug, Error)]
pub enum UrlError {
    #[error("Invalid or incomplete connection URL")]
    Invalid,

    #[error(
        "Invalid value `{}' for connection URL parameter `{}'",
        value, param
    )]
    InvalidParamValue { param: String, value: String },

    #[error("URL parse error: {}", _0)]
    Parse(#[source] ParseError),

    #[error("Unknown connection URL parameter `{}'", param)]
    UnknownParameter { param: String },

    #[error("Unsupported connection URL scheme `{}'", scheme)]
    UnsupportedScheme { scheme: String },
}

/// This type enumerates driver errors.
#[derive(Debug, Error)]
pub enum DriverError {
    #[error("Varint overflows a 64-bit integer.")]
    Overflow,

    #[error("Unknown packet 0x{:x}.", packet)]
    UnknownPacket { packet: u64 },

    #[error("Unexpected packet.")]
    UnexpectedPacket,

    #[error("Timeout error.")]
    Timeout,

    #[error("Invalid utf-8 sequence.")]
    Utf8Error(Utf8Error),
}

/// This type enumerates cast from sql type errors.
#[derive(Debug, Error)]
pub enum FromSqlError {
    #[error("SqlType::{} cannot be cast to {}.", src, dst)]
    InvalidType {
        src: Cow<'static, str>,
        dst: Cow<'static, str>,
    },

    #[error("Out of range.")]
    OutOfRange,

    #[error("Unsupported operation.")]
    UnsupportedOperation,
}

impl From<DriverError> for Error {
    fn from(err: DriverError) -> Self {
        Error::Driver(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<ServerError> for Error {
    fn from(err: ServerError) -> Self {
        Error::Server(err)
    }
}

impl From<UrlError> for Error {
    fn from(err: UrlError) -> Self {
        Error::Url(err)
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Other(Cow::from(err))
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error::Other(err.to_string().into())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::Other(err.to_string().into())
    }
}

impl From<TimerError> for Error {
    fn from(err: TimerError) -> Self {
        Error::Other(err.to_string().into())
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::Url(UrlError::Parse(err))
    }
}

impl From<TimeoutError<Error>> for Error {
    fn from(err: TimeoutError<Self>) -> Self {
        match err.into_inner() {
            None => Error::Driver(DriverError::Timeout),
            Some(inner) => inner,
        }
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error::Driver(DriverError::Utf8Error(err))
    }
}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Error::Connection(err)
    }
}

impl<S> Into<Poll<Option<Packet<S>>, Error>> for Error {
    fn into(self) -> Poll<Option<Packet<S>>, Error> {
        let mut this = self;

        if let Error::Io(ref mut e) = &mut this {
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(Async::NotReady);
            }

            let me = mem::replace(e, io::Error::from(io::ErrorKind::Other));
            return Err(Error::Io(me));
        }

        warn!("ERROR: {:?}", this);
        Err(this)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(error) => error,
            e => io::Error::new(io::ErrorKind::Other, e.to_string()),
        }
    }
}
