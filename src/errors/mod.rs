use std::{borrow::Cow, io, result, str::Utf8Error, string::FromUtf8Error};

use thiserror::Error;
#[cfg(feature = "tokio_io")]
use tokio::time::error::Elapsed;
use url::ParseError;

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

    #[error("Connection broken")]
    Broken,
}

/// This type enumerates connection URL errors.
#[derive(Debug, Error)]
pub enum UrlError {
    #[error("Invalid or incomplete connection URL")]
    Invalid,

    #[error("Invalid value `{}' for connection URL parameter `{}'", value, param)]
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

    #[error("Deserialize error: `{}`", _0)]
    Deserialize(Cow<'static, str>),
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

impl Error {
    pub(crate) fn is_would_block(&self) -> bool {
        if let Error::Io(ref e) = self {
            if e.kind() == io::ErrorKind::WouldBlock {
                return true;
            }
        }
        false
    }
}

impl From<ConnectionError> for Error {
    fn from(error: ConnectionError) -> Self {
        Error::Connection(error)
    }
}

#[cfg(feature = "tls")]
impl From<native_tls::Error> for ConnectionError {
    fn from(error: native_tls::Error) -> Self {
        ConnectionError::TlsError(error)
    }
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

#[cfg(feature = "tokio_io")]
impl From<Elapsed> for Error {
    fn from(_err: Elapsed) -> Self {
        Error::Driver(DriverError::Timeout)
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::Url(UrlError::Parse(err))
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

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error::Driver(DriverError::Utf8Error(err))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn to_std_error_without_recursion() {
        let src_err: super::Error = From::from("Somth went wrong.");
        let dst_err: Box<dyn std::error::Error> = src_err.into();
        assert_eq!(dst_err.to_string(), "Other error: `Somth went wrong.`");
    }

    #[test]
    fn to_io_error_without_recursion() {
        let src_err: super::Error = From::from("Somth went wrong.");
        let dst_err: std::io::Error = src_err.into();
        assert_eq!(dst_err.to_string(), "Other error: `Somth went wrong.`");
    }
}
