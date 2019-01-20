use std::{borrow::Cow, fmt};

use chrono_tz::Tz;
use hostname::get_hostname;

use crate::errors::{Error, ServerError};

pub(crate) use self::{
    cmd::Cmd,
    date_converter::DateConverter,
    from_sql::FromSql,
    marshal::Marshal,
    options::{IntoOptions, OptionsSource},
    stat_buffer::StatBuffer,
    unmarshal::Unmarshal,
    value::Value,
    value_ref::ValueRef,
};
pub use self::{
    block::{Block, Row, Rows},
    column::Column,
    options::Options,
    query::Query,
    query_result::QueryResult,
};

pub(crate) mod column;
mod marshal;
mod stat_buffer;
mod unmarshal;

mod from_sql;
mod value;
mod value_ref;

mod block;
mod cmd;

mod date_converter;
mod query;
mod query_result;

mod options;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub(crate) struct Progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
}

#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub(crate) struct ProfileInfo {
    pub rows: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub applied_limit: bool,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: bool,
}

#[derive(Clone, PartialEq)]
pub(crate) struct ServerInfo {
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

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) server_info: ServerInfo,
    pub(crate) hostname: String,
    pub(crate) options: OptionsSource,
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            name: String::new(),
            revision: 0,
            minor_version: 0,
            major_version: 0,
            timezone: Tz::Zulu,
        }
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context")
            .field("options", &self.options)
            .field("hostname", &self.hostname)
            .finish()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self {
            server_info: ServerInfo::default(),
            hostname: get_hostname().unwrap(),
            options: OptionsSource::default(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Packet<S> {
    Hello(S, ServerInfo),
    Pong(S),
    Progress(Progress),
    ProfileInfo(ProfileInfo),
    Exception(ServerError),
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
    pub fn to_string(self) -> Cow<'static, str> {
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
        write!(f, "{}", Self::to_string(*self))
    }
}

#[test]
fn test_display() {
    let expected = "UInt8".to_string();
    let actual = format!("{}", SqlType::UInt8);
    assert_eq!(expected, actual);
}

/// Library generic result type.
pub(crate) type ClickhouseResult<T> = Result<T, Error>;
