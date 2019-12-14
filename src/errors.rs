use std::{borrow::Cow, io, result, str::Utf8Error, string::FromUtf8Error};

use failure::*;
#[cfg(feature = "tokio_io")]
use tokio::time::Elapsed;
use url::ParseError;

/// Result type alias for this library.
pub type Result<T> = result::Result<T, Error>;

/// This type enumerates library errors.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Driver error: `{}`", _0)]
    Driver(#[cause] DriverError),

    #[fail(display = "Input/output error: `{}`", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "Connections error: `{}`", _0)]
    Connection(#[cause] ConnectionError),

    #[fail(display = "Other error: `{}`", _0)]
    Other(#[cause] failure::Error),

    #[fail(display = "Server error: `{}`", _0)]
    Server(#[cause] ServerError),

    #[fail(display = "URL error: `{}`", _0)]
    Url(#[cause] UrlError),

    #[fail(display = "From SQL error: `{}`", _0)]
    FromSql(#[cause] FromSqlError),
}

/// This type represents Clickhouse server error.
#[derive(Debug, Fail, Clone)]
#[fail(display = "ERROR {} ({}): {}", name, code, message)]
pub struct ServerError {
    pub code: u32,
    pub name: String,
    pub message: String,
    pub stack_trace: String,
}

/// This type enumerates connection errors.
#[derive(Debug, Fail)]
pub enum ConnectionError {
    #[fail(display = "TLS connection requires hostname to be provided")]
    TlsHostNotProvided,

    #[fail(display = "Input/output error: `{}`", _0)]
    IoError(#[cause] io::Error),

    #[cfg(feature = "tls")]
    #[fail(display = "TLS connection error: `{}`", _0)]
    TlsError(#[cause] native_tls::Error),
}

/// This type enumerates connection URL errors.
#[derive(Debug, Fail)]
pub enum UrlError {
    #[fail(display = "Invalid or incomplete connection URL")]
    Invalid,

    #[fail(
        display = "Invalid value `{}' for connection URL parameter `{}'",
        value, param
    )]
    InvalidParamValue { param: String, value: String },

    #[fail(display = "URL parse error: {}", _0)]
    Parse(#[cause] ParseError),

    #[fail(display = "Unknown connection URL parameter `{}'", param)]
    UnknownParameter { param: String },

    #[fail(display = "Unsupported connection URL scheme `{}'", scheme)]
    UnsupportedScheme { scheme: String },
}

/// This type enumerates driver errors.
#[derive(Debug, Fail)]
pub enum DriverError {
    #[fail(display = "Varint overflows a 64-bit integer.")]
    Overflow,

    #[fail(display = "Unknown packet 0x{:x}.", packet)]
    UnknownPacket { packet: u64 },

    #[fail(display = "Unexpected packet.")]
    UnexpectedPacket,

    #[fail(display = "Timeout error.")]
    Timeout,

    #[fail(display = "Invalid utf-8 sequence.")]
    Utf8Error(Utf8Error),
}

/// This type enumerates cast from sql type errors.
#[derive(Debug, Fail)]
pub enum FromSqlError {
    #[fail(display = "SqlType::{} cannot be cast to {}.", src, dst)]
    InvalidType {
        src: Cow<'static, str>,
        dst: Cow<'static, str>,
    },

    #[fail(display = "Out of range.")]
    OutOfRange,

    #[fail(display = "Unsupported operation.")]
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
        Error::Other(failure::Context::new(err).into())
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error::Other(failure::Context::new(err.to_string()).into())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::Other(failure::Context::new(err).into())
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

impl From<Error> for Box<dyn std::error::Error> {
    fn from(err: Error) -> Self {
        Box::<dyn std::error::Error>::from(err.to_string())
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error::Driver(DriverError::Utf8Error(err))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    #[test]
    fn to_std_error_without_recursion() {
        let src_err: super::Error = From::from("Somth went wrong.");
        let dst_err: Box<dyn std::error::Error> = src_err.into();
        assert_eq!(dst_err.description(), "Other error: `Somth went wrong.`");
    }

    #[test]
    fn to_io_error_without_recursion() {
        let src_err: super::Error = From::from("Somth went wrong.");
        let dst_err: std::io::Error = src_err.into();
        assert_eq!(dst_err.description(), "Other error: `Somth went wrong.`");
    }
}
