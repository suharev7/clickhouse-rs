use std::{borrow::Cow, io, mem, string::FromUtf8Error};

use failure::*;
use tokio::prelude::*;
use tokio_timer::timeout::Error as TimeoutError;
use tokio_timer::Error as TimerError;
use url::ParseError;

use crate::types::Packet;
use std::str::Utf8Error;

/// This type enumerates library errors.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Driver error: `{}`", _0)]
    Driver(#[cause] DriverError),

    #[fail(display = "Input/output error: `{}`", _0)]
    Io(#[cause] io::Error),

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

impl From<TimerError> for Error {
    fn from(err: TimerError) -> Self {
        Error::Other(failure::Context::new(err).into())
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
