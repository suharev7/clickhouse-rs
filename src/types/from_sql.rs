use chrono::{prelude::*, Duration};
use chrono_tz::Tz;
use either::Either;
use std::{
    collections::HashMap,
    hash::Hash,
    net::{Ipv4Addr, Ipv6Addr},
};

use crate::{
    errors::{Error, FromSqlError, Result},
    types::{
        column::datetime64::to_datetime,
        value::{decode_ipv4, decode_ipv6},
        Decimal, Enum16, Enum8, SqlType, ValueRef,
    },
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

impl<'a> FromSql<'a> for Enum8 {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Enum8(_enum_values, e) => Ok(e),
            _ => {
                let from = SqlType::from(value.clone()).to_string();

                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Enum8".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Enum16 {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Enum16(_enum_values, e) => Ok(e),
            _ => {
                let from = SqlType::from(value.clone()).to_string();

                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Enum16".into(),
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

impl<'a, K, V> FromSql<'a> for HashMap<K, V>
where
    K: FromSql<'a> + Eq + PartialEq + Hash,
    V: FromSql<'a>,
{
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        if let ValueRef::Map(_k, _v, hm) = value {
            let mut res = HashMap::with_capacity(hm.capacity());

            for (k, v) in hm.iter() {
                res.insert(K::from_sql(k.clone())?, V::from_sql(v.clone())?);
            }

            return Ok(res);
        }

        Err(Error::from("ohh no"))
    }
}

impl<'a> FromSql<'a> for Ipv4Addr {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Ipv4(ip) => Ok(decode_ipv4(&ip)),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Ipv4".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for Ipv6Addr {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Ipv6(ip) => Ok(decode_ipv6(&ip)),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Ipv6".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for uuid::Uuid {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Uuid(row) => {
                let mut buffer = row;
                buffer[..8].reverse();
                buffer[8..].reverse();
                Ok(uuid::Uuid::from_bytes(buffer))
            }
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Uuid".into(),
                }))
            }
        }
    }
}

macro_rules! from_sql_vec_impl {
    // Base case: Vec<T>
    (
        $($t:ty: $k:pat => $f:expr),*
    ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::Array($k, vs) => {
                            let f: fn(ValueRef<'a>) -> FromSqlResult<$t> = $f;
                            let mut result = Vec::with_capacity(vs.len());
                            for r in vs.iter() {
                                let value: $t = f(r.clone())?;
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
            // Recursive case: Vec<Vec<T>>
            impl<'a> FromSql<'a> for Vec<Vec<$t>> {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::Array(SqlType::Array(_), outer_vs) => {
                            let mut result = Vec::with_capacity(outer_vs.len());
                            for outer_v in outer_vs.iter() {
                                match outer_v {
                                    ValueRef::Array($k, inner_vs) => {
                                        let f: fn(ValueRef<'a>) -> FromSqlResult<$t> = $f;
                                        let inner: Vec<$t> = inner_vs.iter().map(|inner_v| f(inner_v.clone())).collect::<std::result::Result<Vec<$t>, _>>()?;
                                        result.push(inner);
                                    }
                                    _ => {
                                        return Err(Error::FromSql(FromSqlError::InvalidType {
                                            src: SqlType::from(outer_v.clone()).to_string(),
                                            dst: format!("Vec<Vec<{}>>", stringify!($t)).into(),
                                        }));
                                    }
                                }
                            }
                            Ok(result)
                        }
                        _ => Err(Error::FromSql(FromSqlError::InvalidType {
                            src: SqlType::from(value.clone()).to_string(),
                            dst: format!("Vec<Vec<{}>>", stringify!($t)).into(),
                        })),
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    &'a str: SqlType::String => |r| r.as_str(),
    String: SqlType::String => |r| r.as_string(),
    &'a [u8]: SqlType::String => |r| r.as_bytes(),
    Vec<u8>: SqlType::String => |r| r.as_bytes().map(<[u8]>::to_vec),
    NaiveDate: SqlType::Date => |r| Ok(r.into()),
    DateTime<Tz>: SqlType::DateTime(_) => |r| Ok(r.into()),
    Enum8: SqlType::Enum8(_) => |r| Ok(r.into()),
    Enum16: SqlType::Enum16(_) => |r| Ok(r.into())
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Array(SqlType::UInt8, vs) => {
                let mut result = Vec::with_capacity(vs.len());
                for r in vs.iter() {
                    result.push(r.clone().into());
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
    bool: Bool,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    i128: Int128,

    u16: UInt16,
    u32: UInt32,
    u64: UInt64,
    u128: UInt128,

    f32: Float32,
    f64: Float64
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

impl<'a> FromSql<'a> for NaiveDate {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Date(v) => NaiveDate::from_ymd_opt(1970, 1, 1)
                .map(|unix_epoch| unix_epoch + Duration::days(v.into()))
                .ok_or(Error::FromSql(FromSqlError::OutOfRange)),
            _ => {
                let from = SqlType::from(value).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "NaiveDate".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for DateTime<Tz> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::DateTime(v, tz) => {
                let time = tz.timestamp_opt(i64::from(v), 0).unwrap();
                Ok(time)
            }
            ValueRef::DateTime64(v, params) => {
                let (precision, tz) = *params;
                Ok(to_datetime(v, precision, tz))
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
    bool: Bool,

    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,
    u128: UInt128,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    i128: Int128,

    f32: Float32,
    f64: Float64
}

#[cfg(test)]
mod test {
    use crate::types::{from_sql::FromSql, DateTimeType, SqlType, ValueRef};
    use chrono::prelude::*;
    use chrono_tz::Tz;
    use either::Either;

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
                format!("{e}")
            ),
        }
    }

    #[test]
    fn null_to_datetime() {
        let null_value = ValueRef::Nullable(Either::Left(
            SqlType::DateTime(DateTimeType::DateTime32).into(),
        ));
        let date = Option::<DateTime<Tz>>::from_sql(null_value);
        assert_eq!(date.unwrap(), None);
    }
}
