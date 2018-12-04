use std::borrow::Cow;
use std::error;
use std::fmt;
use std::io;
use std::mem;
use std::string::FromUtf8Error;

use chrono_tz::Tz;
use futures::{Async, Poll};
use hostname::get_hostname;

use Block;

pub use self::cmd::Cmd;
pub use self::date_converter::DateConverter;
pub use self::from_sql::{FromSql, FromSqlError, FromSqlResult};
pub use self::marshal::Marshal;
pub use self::query::Query;
pub use self::stat_buffer::StatBuffer;
pub use self::unmarshal::Unmarshal;
pub use self::value::Value;
pub use self::value_ref::ValueRef;

mod marshal;
mod stat_buffer;
mod unmarshal;

mod from_sql;
mod value;
mod value_ref;

mod cmd;

mod date_converter;
pub mod query;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct Progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
}

#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub struct ProfileInfo {
    pub rows: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub applied_limit: bool,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: bool,
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct ExceptionRepr {
    pub code: u32,
    pub name: String,
    pub message: String,
    pub stack_trace: String,
}

#[derive(Clone, PartialEq)]
pub struct ServerInfo {
    pub name: String,
    pub revision: u64,
    pub minor_version: u64,
    pub major_version: u64,
    pub timezone: Tz,
}

impl fmt::Debug for ServerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {}.{}.{} ({:?})",
            self.name, self.major_version, self.minor_version, self.revision, self.timezone
        )
    }
}

#[derive(Debug, Clone)]
pub struct Context {
    pub server_info: ServerInfo,
    pub database: String,
    pub username: String,
    pub password: String,
    pub hostname: String,
}

impl Default for ServerInfo {
    fn default() -> Self {
        ServerInfo {
            name: String::new(),
            revision: 0,
            minor_version: 0,
            major_version: 0,
            timezone: Tz::Zulu,
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Context {
            server_info: ServerInfo::default(),
            database: "default".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            hostname: get_hostname().unwrap(),
        }
    }
}

#[derive(Clone)]
pub enum Packet<S> {
    Hello(S, ServerInfo),
    Pong(S),
    Progress(Progress),
    ProfileInfo(ProfileInfo),
    Exception(ExceptionRepr),
    Block(Block),
    Eof(S),
}

impl<S> fmt::Debug for Packet<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Packet::Hello(_, info) => write!(f, "Hello({:?})", info),
            Packet::Pong(_) => write!(f, "Pong"),
            Packet::Progress(p) => write!(f, "Progress({:?})", p),
            Packet::ProfileInfo(info) => write!(f, "ProfileInfo({:?})", info),
            Packet::Exception(e) => write!(f, "Exception({:?})", e),
            Packet::Block(b) => write!(f, "Block({:?})", b),
            Packet::Eof(_) => write!(f, "Eof"),
        }
    }
}

impl<S> Packet<S> {
    pub fn bind<N>(self, transport: &mut Option<N>) -> Packet<N> {
        match self {
            Packet::Hello(_, server_info) => Packet::Hello(transport.take().unwrap(), server_info),
            Packet::Pong(_) => Packet::Pong(transport.take().unwrap()),
            Packet::Progress(progress) => Packet::Progress(progress),
            Packet::ProfileInfo(profile_info) => Packet::ProfileInfo(profile_info),
            Packet::Exception(exception) => Packet::Exception(exception),
            Packet::Block(block) => Packet::Block(block),
            Packet::Eof(_) => Packet::Eof(transport.take().unwrap()),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SqlType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    Float32,
    Float64,
    Date,
    DateTime,
}

impl SqlType {
    pub fn to_string(&self) -> Cow<'static, str> {
        match self {
            SqlType::UInt8 => "UInt8".into(),
            SqlType::UInt16 => "UInt16".into(),
            SqlType::UInt32 => "UInt32".into(),
            SqlType::UInt64 => "UInt64".into(),
            SqlType::Int8 => "Int8".into(),
            SqlType::Int16 => "Int16".into(),
            SqlType::Int32 => "Int32".into(),
            SqlType::Int64 => "Int64".into(),
            SqlType::String => "String".into(),
            SqlType::Float32 => "Float32".into(),
            SqlType::Float64 => "Float64".into(),
            SqlType::Date => "Date".into(),
            SqlType::DateTime => "DateTime".into(),
        }
    }
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[test]
fn test_display() {
    let expected = "UInt8".to_string();
    let actual = format!("{}", SqlType::UInt8);
    assert_eq!(expected, actual);
}

pub enum ClickhouseError {
    Overflow,
    UnknownPacket(u64),
    IoError(io::Error),
    Utf8Error(FromUtf8Error),
    ResponseError(&'static str),
    Internal(ExceptionRepr),
    UnexpectedPacket,
}

impl From<io::Error> for ClickhouseError {
    fn from(error: io::Error) -> ClickhouseError {
        ClickhouseError::IoError(error)
    }
}

impl From<FromUtf8Error> for ClickhouseError {
    fn from(error: FromUtf8Error) -> ClickhouseError {
        ClickhouseError::Utf8Error(error)
    }
}

impl From<ClickhouseError> for io::Error {
    fn from(error: ClickhouseError) -> io::Error {
        match error {
            ClickhouseError::IoError(err) => err,
            _ => io::Error::new(io::ErrorKind::Other, error),
        }
    }
}

impl error::Error for ClickhouseError {
    fn description(&self) -> &str {
        match self {
            ClickhouseError::Overflow => "Varint overflows a 64-bit integer",
            ClickhouseError::UnknownPacket(_) => "Unknown packet",
            ClickhouseError::IoError(err) => err.description(),
            ClickhouseError::Utf8Error(err) => err.description(),
            ClickhouseError::ResponseError(message) => &message,
            ClickhouseError::Internal(ref e) => &e.message,
            ClickhouseError::UnexpectedPacket => "Unexpected packet",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            ClickhouseError::IoError(ref err) => Some(err as &error::Error),
            ClickhouseError::Utf8Error(ref err) => Some(err as &error::Error),
            _ => None,
        }
    }
}

impl fmt::Display for ClickhouseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            ClickhouseError::Overflow => write!(f, "Varint overflows a 64-bit integer"),
            ClickhouseError::UnknownPacket(packet) => write!(f, "Unknown packet: {}", packet),
            ClickhouseError::IoError(error) => write!(f, "IoError: {}", error),
            ClickhouseError::Utf8Error(error) => write!(f, "Utf8Error: {}", error),
            ClickhouseError::ResponseError(message) => write!(f, "ResponseError: {}", message),
            ClickhouseError::Internal(e) => write!(f, "{}", e.message),
            ClickhouseError::UnexpectedPacket => write!(f, "Unexpected packet"),
        }
    }
}

impl fmt::Debug for ClickhouseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt::Display::fmt(self, f)
    }
}

impl<S> Into<Poll<Option<Packet<S>>, io::Error>> for ClickhouseError {
    fn into(self) -> Poll<Option<Packet<S>>, io::Error> {
        let mut this = self;

        if let ClickhouseError::IoError(ref mut e) = &mut this {
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(Async::NotReady);
            }

            let me = mem::replace(e, io::Error::from(io::ErrorKind::Other));
            return Err(me);
        }

        warn!("ERROR: {:?}", this);
        Err(io::Error::new(io::ErrorKind::Other, this))
    }
}

/// Library generic result type.
pub type ClickhouseResult<T> = Result<T, ClickhouseError>;
