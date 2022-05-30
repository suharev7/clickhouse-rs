use std::{borrow::Cow, collections::HashMap, fmt, mem, pin::Pin, str::FromStr, sync::Mutex};

use chrono::prelude::*;
use chrono_tz::Tz;
use hostname::get;

use lazy_static::lazy_static;

use crate::errors::ServerError;

pub use self::{
    block::{Block, RCons, RNil, Row, RowBuilder, Rows},
    column::{Column, ColumnType, Complex, Simple},
    decimal::Decimal,
    enums::{Enum16, Enum8},
    from_sql::{FromSql, FromSqlResult},
    options::Options,
    query::Query,
    query_result::QueryResult,
    value::Value,
    value_ref::ValueRef,
};

pub(crate) use self::{
    cmd::Cmd,
    date_converter::DateConverter,
    marshal::Marshal,
    options::{IntoOptions, OptionsSource},
    stat_buffer::StatBuffer,
    unmarshal::Unmarshal,
};

pub(crate) mod column;
mod marshal;
mod stat_buffer;
mod unmarshal;

mod from_sql;
mod value;
mod value_ref;

pub(crate) mod block;
mod cmd;

mod date_converter;
mod query;
pub(crate) mod query_result;

mod decimal;
mod enums;
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
            hostname: get().unwrap().into_string().unwrap(),
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

pub trait HasSqlType {
    fn get_sql_type() -> SqlType;
}

macro_rules! has_sql_type {
    ( $( $t:ty : $k:expr ),* ) => {
        $(
            impl HasSqlType for $t {
                fn get_sql_type() -> SqlType {
                    $k
                }
            }
        )*
    };
}

has_sql_type! {
    u8: SqlType::UInt8,
    u16: SqlType::UInt16,
    u32: SqlType::UInt32,
    u64: SqlType::UInt64,
    i8: SqlType::Int8,
    i16: SqlType::Int16,
    i32: SqlType::Int32,
    i64: SqlType::Int64,
    &str: SqlType::String,
    String: SqlType::String,
    f32: SqlType::Float32,
    f64: SqlType::Float64,
    Date<Tz>: SqlType::Date,
    DateTime<Tz>: SqlType::DateTime(DateTimeType::DateTime32)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateTimeType {
    DateTime32,
    DateTime64(u32, Tz),
    Chrono,
}

#[derive(Debug, Copy, Clone, PartialOrd, Eq, PartialEq, Hash)]
pub enum SimpleAggFunc {
    Any,
    AnyLast,
    Min,
    Max,
    Sum,
    SumWithOverflow,
    GroupBitAnd,
    GroupBitOr,
    GroupBitXor,
    GroupArrayArray,
    GroupUniqArrayArray,
    SumMap,
    MinMap,
    MaxMap,
    ArgMin,
    ArgMax,
}

impl From<SimpleAggFunc> for &str {
    fn from(source: SimpleAggFunc) -> &'static str {
        match source {
            SimpleAggFunc::Any => "any",
            SimpleAggFunc::AnyLast => "anyLast",
            SimpleAggFunc::Min => "min",
            SimpleAggFunc::Max => "max",
            SimpleAggFunc::Sum => "sum",
            SimpleAggFunc::SumWithOverflow => "sumWithOverflow",
            SimpleAggFunc::GroupBitAnd => "groupBitAnd",
            SimpleAggFunc::GroupBitOr => "groupBitOr",
            SimpleAggFunc::GroupBitXor => "groupBitXor",
            SimpleAggFunc::GroupArrayArray => "groupArrayArray",
            SimpleAggFunc::GroupUniqArrayArray => "groupUniqArrayArray",
            SimpleAggFunc::SumMap => "sumMap",
            SimpleAggFunc::MinMap => "minMap",
            SimpleAggFunc::MaxMap => "maxMap",
            SimpleAggFunc::ArgMin => "argMin",
            SimpleAggFunc::ArgMax => "argMax",
        }
    }
}

impl FromStr for SimpleAggFunc {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => Ok(SimpleAggFunc::Any),
            "anyLast" => Ok(SimpleAggFunc::AnyLast),
            "min" => Ok(SimpleAggFunc::Min),
            "max" => Ok(SimpleAggFunc::Max),
            "sum" => Ok(SimpleAggFunc::Sum),
            "sumWithOverflow" => Ok(SimpleAggFunc::SumWithOverflow),
            "groupBitAnd" => Ok(SimpleAggFunc::GroupBitAnd),
            "groupBitOr" => Ok(SimpleAggFunc::GroupBitOr),
            "groupBitXor" => Ok(SimpleAggFunc::GroupBitXor),
            "groupArrayArray" => Ok(SimpleAggFunc::GroupArrayArray),
            "groupUniqArrayArray" => Ok(SimpleAggFunc::GroupUniqArrayArray),
            "sumMap" => Ok(SimpleAggFunc::SumMap),
            "minMap" => Ok(SimpleAggFunc::MinMap),
            "maxMap" => Ok(SimpleAggFunc::MaxMap),
            "argMin" => Ok(SimpleAggFunc::ArgMin),
            "argMax" => Ok(SimpleAggFunc::ArgMax),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
    FixedString(usize),
    Float32,
    Float64,
    Date,
    DateTime(DateTimeType),
    Ipv4,
    Ipv6,
    Uuid,
    Nullable(&'static SqlType),
    Array(&'static SqlType),
    Decimal(u8, u8),
    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),
    SimpleAggregateFunction(SimpleAggFunc, &'static SqlType),
    Map(&'static SqlType, &'static SqlType),
}

lazy_static! {
    static ref TYPES_CACHE: Mutex<HashMap<SqlType, Pin<Box<SqlType>>>> = Mutex::new(HashMap::new());
}

impl From<SqlType> for &'static SqlType {
    fn from(value: SqlType) -> Self {
        match value {
            SqlType::UInt8 => &SqlType::UInt8,
            SqlType::UInt16 => &SqlType::UInt16,
            SqlType::UInt32 => &SqlType::UInt32,
            SqlType::UInt64 => &SqlType::UInt64,
            SqlType::Int8 => &SqlType::Int8,
            SqlType::Int16 => &SqlType::Int16,
            SqlType::Int32 => &SqlType::Int32,
            SqlType::Int64 => &SqlType::Int64,
            SqlType::String => &SqlType::String,
            SqlType::Float32 => &SqlType::Float32,
            SqlType::Float64 => &SqlType::Float64,
            SqlType::Date => &SqlType::Date,
            _ => {
                let mut guard = TYPES_CACHE.lock().unwrap();
                loop {
                    if let Some(value_ref) = guard.get(&value.clone()) {
                        return unsafe { mem::transmute(value_ref.as_ref()) };
                    }
                    guard.insert(value.clone(), Box::pin(value.clone()));
                }
            }
        }
    }
}

impl SqlType {
    pub(crate) fn is_datetime(&self) -> bool {
        matches!(self, SqlType::DateTime(_))
    }

    pub fn to_string(&self) -> Cow<'static, str> {
        match self.clone() {
            SqlType::UInt8 => "UInt8".into(),
            SqlType::UInt16 => "UInt16".into(),
            SqlType::UInt32 => "UInt32".into(),
            SqlType::UInt64 => "UInt64".into(),
            SqlType::Int8 => "Int8".into(),
            SqlType::Int16 => "Int16".into(),
            SqlType::Int32 => "Int32".into(),
            SqlType::Int64 => "Int64".into(),
            SqlType::String => "String".into(),
            SqlType::FixedString(str_len) => format!("FixedString({})", str_len).into(),
            SqlType::Float32 => "Float32".into(),
            SqlType::Float64 => "Float64".into(),
            SqlType::Date => "Date".into(),
            SqlType::DateTime(DateTimeType::DateTime64(precision, tz)) => {
                format!("DateTime64({}, '{:?}')", precision, tz).into()
            }
            SqlType::DateTime(_) => "DateTime".into(),
            SqlType::Ipv4 => "IPv4".into(),
            SqlType::Ipv6 => "IPv6".into(),
            SqlType::Uuid => "UUID".into(),
            SqlType::Nullable(nested) => format!("Nullable({})", &nested).into(),
            SqlType::SimpleAggregateFunction(func, nested) => {
                let func_str: &str = func.into();
                format!("SimpleAggregateFunction({}, {})", func_str, &nested).into()
            }
            SqlType::Array(nested) => format!("Array({})", &nested).into(),
            SqlType::Decimal(precision, scale) => {
                format!("Decimal({}, {})", precision, scale).into()
            }
            SqlType::Enum8(values) => {
                let a: Vec<String> = values
                    .iter()
                    .map(|(name, value)| format!("'{}' = {}", name, value))
                    .collect();
                format!("Enum8({})", a.join(",")).into()
            }
            SqlType::Enum16(values) => {
                let a: Vec<String> = values
                    .iter()
                    .map(|(name, value)| format!("'{}' = {}", name, value))
                    .collect();
                format!("Enum16({})", a.join(",")).into()
            }
            SqlType::Map(k, v) => format!("Map({}, {})", &k, &v).into(),
        }
    }

    pub(crate) fn level(&self) -> u8 {
        match self {
            SqlType::Nullable(inner) => 1 + inner.level(),
            SqlType::Array(inner) => 1 + inner.level(),
            SqlType::Map(key, _value) => 1 + key.level(),
            _ => 0,
        }
    }
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Self::to_string(self))
    }
}

#[test]
fn test_display() {
    let expected = "UInt8".to_string();
    let actual = format!("{}", SqlType::UInt8);
    assert_eq!(expected, actual);
}

#[test]
fn test_to_string() {
    let expected: Cow<'static, str> = "Nullable(UInt8)".into();
    let actual = SqlType::Nullable(&SqlType::UInt8).to_string();
    assert_eq!(expected, actual)
}
