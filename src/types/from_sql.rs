use std::convert;
use std::error::Error;
use std::fmt;
use std::io;

use chrono::prelude::*;
use chrono_tz::Tz;

use crate::types::{SqlType, ValueRef};
use std::borrow::Cow;

#[derive(Debug)]
pub enum FromSqlError {
    InvalidType(Cow<'static, str>, &'static str),
    OutOfRange,
    Other(Box<Error + Send + Sync>),
}

impl fmt::Display for FromSqlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FromSqlError::InvalidType(src, dst) => {
                write!(f, "SqlType::{} cannot be cast to {}.", src, dst)
            }
            FromSqlError::OutOfRange => write!(f, "Out of range"),
            FromSqlError::Other(ref err) => err.fmt(f),
        }
    }
}

impl Error for FromSqlError {
    fn description(&self) -> &str {
        match *self {
            FromSqlError::InvalidType(_, _) => "invalid type",
            FromSqlError::OutOfRange => "out of type",
            FromSqlError::Other(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            FromSqlError::Other(ref err) => err.cause(),
            _ => None,
        }
    }
}

impl convert::From<FromSqlError> for io::Error {
    fn from(cause: FromSqlError) -> Self {
        io::Error::new(io::ErrorKind::Other, cause)
    }
}

pub type FromSqlResult<T> = Result<T, FromSqlError>;

pub trait FromSql<'a>: Sized {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self>;
}

macro_rules! from_sql_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for $t {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::$k(v) => Ok(v),
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(FromSqlError::InvalidType(from, stringify!($t)))
                        }
                    }
                }
            }
        )*
    };
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a str> {
        value.as_str()
    }
}

impl<'a> FromSql<'a> for String {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<String> {
        value.as_str().map(str::to_string)
    }
}

impl<'a> FromSql<'a> for DateTime<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::DateTime(v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(FromSqlError::InvalidType(from, "DateTime<Tz>"))
            }
        }
    }
}

from_sql_impl! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    f32: Float32,
    f64: Float64
}

#[cfg(test)]
mod test {
    use crate::types::from_sql::FromSql;
    use crate::types::ValueRef;

    #[test]
    fn test_u8() {
        let v = ValueRef::from(42_u8);
        let actual = u8::from_sql(v).unwrap();
        assert_eq!(actual, 42_u8);
    }

    #[test]
    fn test_bad_convert() {
        let v = ValueRef::from(42_u16);
        match u32::from_sql(v) {
            Ok(_) => panic!("should fail"),
            Err(e) => assert_eq!(
                "SqlType::UInt16 cannot be cast to u32.".to_string(),
                format!("{}", e)
            ),
        }
    }
}
