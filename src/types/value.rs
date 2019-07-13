use std::{convert, fmt, mem, str};

use chrono::prelude::*;
use chrono_tz::Tz;

use crate::types::{column::Either, DateConverter, SqlType};

pub(crate) type AppDateTime = DateTime<Tz>;
pub(crate) type AppDate = Date<Tz>;

/// Client side representation of a value of Clickhouse column.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(Vec<u8>),
    Float32(f32),
    Float64(f64),
    Date(Date<Tz>),
    DateTime(DateTime<Tz>),
    Nullable(Either<SqlType, Box<Value>>),
    Array(SqlType, Vec<Value>),
}

impl Value {
    pub(crate) fn default(sql_type: SqlType) -> Value {
        match sql_type {
            SqlType::UInt8 => Value::UInt8(0),
            SqlType::UInt16 => Value::UInt16(0),
            SqlType::UInt32 => Value::UInt32(0),
            SqlType::UInt64 => Value::UInt64(0),
            SqlType::Int8 => Value::Int8(0),
            SqlType::Int16 => Value::Int16(0),
            SqlType::Int32 => Value::Int32(0),
            SqlType::Int64 => Value::Int64(0),
            SqlType::String => Value::String(Vec::default()),
            SqlType::FixedString(str_len) => Value::String(vec![0_u8; str_len]),
            SqlType::Float32 => Value::Float32(0.0),
            SqlType::Float64 => Value::Float64(0.0),
            SqlType::Date => 0_u16.to_date(Tz::Zulu).into(),
            SqlType::DateTime => 0_u32.to_date(Tz::Zulu).into(),
            SqlType::Nullable(inner) => Value::Nullable(Either::Left(*inner)),
            SqlType::Array(inner) => Value::Array(*inner, Vec::default()),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::UInt8(ref v) => fmt::Display::fmt(v, f),
            Value::UInt16(ref v) => fmt::Display::fmt(v, f),
            Value::UInt32(ref v) => fmt::Display::fmt(v, f),
            Value::UInt64(ref v) => fmt::Display::fmt(v, f),
            Value::Int8(ref v) => fmt::Display::fmt(v, f),
            Value::Int16(ref v) => fmt::Display::fmt(v, f),
            Value::Int32(ref v) => fmt::Display::fmt(v, f),
            Value::Int64(ref v) => fmt::Display::fmt(v, f),
            Value::String(ref v) => match str::from_utf8(v) {
                Ok(s) => fmt::Display::fmt(s, f),
                Err(_) => write!(f, "{:?}", v),
            },
            Value::Float32(ref v) => fmt::Display::fmt(v, f),
            Value::Float64(ref v) => fmt::Display::fmt(v, f),
            Value::DateTime(ref time) if f.alternate() => write!(f, "{}", time.to_rfc2822()),
            Value::DateTime(ref time) => write!(f, "{}", time),
            Value::Date(v) if f.alternate() => fmt::Display::fmt(v, f),
            Value::Date(v) => fmt::Display::fmt(&v.format("%Y-%m-%d"), f),
            Value::Nullable(v) => match v {
                Either::Left(_) => write!(f, "NULL"),
                Either::Right(data) => data.fmt(f),
            },
            Value::Array(_, vs) => {
                let cells: Vec<String> = vs.iter().map(|v| format!("{}", v)).collect();
                write!(f, "[{}]", cells.join(", "))
            }
        }
    }
}

impl convert::From<Value> for SqlType {
    fn from(source: Value) -> Self {
        match source {
            Value::UInt8(_) => SqlType::UInt8,
            Value::UInt16(_) => SqlType::UInt16,
            Value::UInt32(_) => SqlType::UInt32,
            Value::UInt64(_) => SqlType::UInt64,
            Value::Int8(_) => SqlType::Int8,
            Value::Int16(_) => SqlType::Int16,
            Value::Int32(_) => SqlType::Int32,
            Value::Int64(_) => SqlType::Int64,
            Value::String(_) => SqlType::String,
            Value::Float32(_) => SqlType::Float32,
            Value::Float64(_) => SqlType::Float64,
            Value::Date(_) => SqlType::Date,
            Value::DateTime(_) => SqlType::DateTime,
            Value::Nullable(d) => match d {
                Either::Left(t) => SqlType::Nullable(t.into()),
                Either::Right(inner) => {
                    let sql_type = SqlType::from(inner.as_ref().to_owned());
                    SqlType::Nullable(sql_type.into())
                }
            },
            Value::Array(t, _) => SqlType::Array(t.into()),
        }
    }
}

impl<T> convert::From<Option<T>> for Value
where
    Value: convert::From<T>,
    T: Default,
{
    fn from(value: Option<T>) -> Value {
        match value {
            None => {
                let default_value: Value = T::default().into();
                Value::Nullable(Either::Left(default_value.into()))
            }
            Some(inner) => Value::Nullable(Either::Right(Box::new(inner.into()))),
        }
    }
}

macro_rules! value_from {
    ( $( $t:ty : $k:ident ),* ) => {
        $(
            impl convert::From<$t> for Value {
                fn from(v: $t) -> Value {
                    Value::$k(v.into())
                }
            }
        )*
    };
}

value_from! {
    AppDateTime: DateTime,
    AppDate: Date,

    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    f32: Float32,
    f64: Float64,

    &[u8]:   String,
    String:  String,
    Vec<u8>: String
}

impl<'a> convert::From<&'a str> for Value {
    fn from(v: &'a str) -> Self {
        Value::String(v.as_bytes().into())
    }
}

impl convert::From<Value> for String {
    fn from(mut v: Value) -> Self {
        if let Value::String(ref mut x) = &mut v {
            let mut tmp = Vec::new();
            mem::swap(x, &mut tmp);
            if let Ok(result) = String::from_utf8(tmp) {
                return result;
            }
        }
        let from = SqlType::from(v);
        panic!("Can't convert Value::{} into String.", from);
    }
}

impl convert::From<Value> for Vec<u8> {
    fn from(v: Value) -> Self {
        match v {
            Value::String(bs) => bs,
            _ => {
                let from = SqlType::from(v);
                panic!("Can't convert Value::{} into Vec<u8>.", from)
            }
        }
    }
}

macro_rules! from_value {
    ( $( $t:ty : $k:ident ),* ) => {
        $(
            impl convert::From<Value> for $t {
                fn from(v: Value) -> $t {
                    if let Value::$k(x) = v {
                        return x;
                    }
                    let from = SqlType::from(v);
                    panic!("Can't convert Value::{} into {}", from, stringify!($t))
                }
            }
        )*
    };
}

from_value! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,
    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    f32: Float32,
    f64: Float64,
    AppDate: Date,
    AppDateTime: DateTime
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono_tz::Tz::{self, UTC};
    use std::fmt;

    use rand::{
        distributions::{Distribution, Standard},
        random,
    };

    fn test_into_t<T>(v: Value, x: &T)
    where
        Value: convert::Into<T>,
        T: PartialEq + fmt::Debug,
    {
        let a: T = v.into();
        assert_eq!(a, *x);
    }

    fn test_from_rnd<T>()
    where
        Value: convert::Into<T>,
        Value: convert::From<T>,
        T: PartialEq + fmt::Debug + Clone,
        Standard: Distribution<T>,
    {
        for _ in 0..100 {
            let value = random::<T>();
            test_into_t::<T>(Value::from(value.clone()), &value);
        }
    }

    fn test_from_t<T>(value: &T)
    where
        Value: convert::Into<T>,
        Value: convert::From<T>,
        T: PartialEq + fmt::Debug + Clone,
    {
        test_into_t::<T>(Value::from(value.clone()), &value);
    }

    macro_rules! test_type {
        ( $( $k:ident : $t:ident ),* ) => {
            $(
                #[test]
                fn $k() {
                    test_from_rnd::<$t>();
                }
            )*
        };
    }

    test_type! {
        test_u8: u8,
        test_u16: u16,
        test_u32: u32,
        test_u64: u64,

        test_i8: i8,
        test_i16: i16,
        test_i32: i32,
        test_i64: i64,

        test_f32: f32,
        test_f64: f64
    }

    #[test]
    fn test_string() {
        test_from_t(&"284222f9-aba2-4b05-bcf5-e4e727fe34d1".to_string());
    }

    #[test]
    fn test_time() {
        test_from_t(&Tz::Africa__Addis_Ababa.ymd(2016, 10, 22).and_hms(12, 0, 0));
    }

    #[test]
    fn test_from_u32() {
        let v = Value::UInt32(32);
        let u: u32 = u32::from(v);
        assert_eq!(u, 32);
    }

    #[test]
    fn test_from_date() {
        let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
        let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

        let d: Value = Value::from(date_value);
        let dt: Value = date_time_value.into();

        assert_eq!(Value::Date(date_value), d);
        assert_eq!(Value::DateTime(date_time_value), dt);
    }

    #[test]
    fn test_string_from() {
        let v = Value::String(b"df47a455-bb3c-4bd6-b2f2-a24be3db36ab".to_vec());
        let u = String::from(v);
        assert_eq!("df47a455-bb3c-4bd6-b2f2-a24be3db36ab".to_string(), u);
    }

    #[test]
    fn test_into_string() {
        let v = Value::String(b"d2384838-dfe8-43ea-b1f7-63fb27b91088".to_vec());
        let u: String = v.into();
        assert_eq!("d2384838-dfe8-43ea-b1f7-63fb27b91088".to_string(), u);
    }

    #[test]
    fn test_into_vec() {
        let v = Value::String(vec![1, 2, 3]);
        let u: Vec<u8> = v.into();
        assert_eq!(vec![1, 2, 3], u);
    }

    #[test]
    fn test_display() {
        assert_eq!("42".to_string(), format!("{}", Value::UInt8(42)));
        assert_eq!("42".to_string(), format!("{}", Value::UInt16(42)));
        assert_eq!("42".to_string(), format!("{}", Value::UInt32(42)));
        assert_eq!("42".to_string(), format!("{}", Value::UInt64(42)));

        assert_eq!("42".to_string(), format!("{}", Value::Int8(42)));
        assert_eq!("42".to_string(), format!("{}", Value::Int16(42)));
        assert_eq!("42".to_string(), format!("{}", Value::Int32(42)));
        assert_eq!("42".to_string(), format!("{}", Value::Int64(42)));

        assert_eq!(
            "text".to_string(),
            format!("{}", Value::String(b"text".to_vec()))
        );

        assert_eq!(
            "\u{1}\u{2}\u{3}".to_string(),
            format!("{}", Value::String(vec![1, 2, 3]))
        );

        assert_eq!(
            "NULL".to_string(),
            format!("{}", Value::Nullable(Either::Left(SqlType::UInt8)))
        );
        assert_eq!(
            "42".to_string(),
            format!(
                "{}",
                Value::Nullable(Either::Right(Box::new(Value::UInt8(42))))
            )
        );

        assert_eq!(
            "[1, 2, 3]".to_string(),
            format!(
                "{}",
                Value::Array(
                    SqlType::Int32,
                    vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
                )
            )
        );
    }

    #[test]
    fn test_default_fixed_str() {
        for n in 0_usize..1000_usize {
            let actual = Value::default(SqlType::FixedString(n));
            let actual_str: String = actual.into();
            assert_eq!(actual_str.len(), n);
            for ch in actual_str.as_bytes() {
                assert_eq!(*ch, 0_u8);
            }
        }
    }
}
