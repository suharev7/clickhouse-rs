use chrono::prelude::*;
use chrono_tz::Tz;

use crate::{
    errors::{Error, FromSqlError, Result},
    types::{column::Either, Decimal, SqlType, ValueRef},
};

pub type FromSqlResult<T> = Result<T>;

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
                            Err(Error::FromSql(FromSqlError::InvalidType { src: from, dst: stringify!($t).into() }))
                        }
                    }
                }
            }
        )*
    };
}

impl<'a> FromSql<'a> for Decimal {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Decimal(v) => Ok(v),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Decimal".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a str> {
        value.as_str()
    }
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a [u8]> {
        value.as_bytes()
    }
}

impl<'a> FromSql<'a> for String {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        value.as_str().map(str::to_string)
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ty: $k:ident => $f:expr ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::Array(SqlType::$k, vs) => {
                            let f: fn(ValueRef<'a>) -> FromSqlResult<$t> = $f;
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let value: $t = f(v.clone())?;
                                result.push(value);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType {
                                src: from,
                                dst: format!("Vec<{}>", stringify!($t)).into(),
                            }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    &'a str: String => |v| v.as_str(),
    String: String => |v| v.as_string(),
    Date<Tz>: Date => |z| Ok(z.into()),
    DateTime<Tz>: DateTime => |z| Ok(z.into())
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Array(SqlType::UInt8, vs) => {
                let mut result = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    result.push(v.clone().into());
                }
                Ok(result)
            }
            _ => value.as_bytes().map(|bs| bs.to_vec()),
        }
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> Result<Self> {
                    match value {
                        ValueRef::Array(SqlType::$k, vs) => {
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let val: $t = v.clone().into();
                                result.push(val);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType { src: from, dst: stringify!($t).into() }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    u16: UInt16,
    u32: UInt32,
    u64: UInt64
}

impl<'a, T> FromSql<'a> for Option<T>
where
    T: FromSql<'a>,
{
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Nullable(e) => match e {
                Either::Left(_) => Ok(None),
                Either::Right(u) => {
                    let value_ref = u.as_ref().clone();
                    Ok(Some(T::from_sql(value_ref)?))
                }
            },
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: stringify!($t).into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Date<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Date(v, tz) => {
                let time = tz.timestamp(i64::from(v) * 24 * 3600, 0);
                Ok(time.date())
            }
            _ => {
                let from = SqlType::from(value).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Date<Tz>".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for DateTime<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::DateTime(v, tz) => {
                let time = tz.timestamp(i64::from(v), 0);
                Ok(time)
            }
            _ => {
                let from = SqlType::from(value).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "DateTime<Tz>".into(),
                }))
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
    use crate::types::{from_sql::FromSql, ValueRef};

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
                "From SQL error: `SqlType::UInt16 cannot be cast to u32.`".to_string(),
                format!("{}", e)
            ),
        }
    }
}
