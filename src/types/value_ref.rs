use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    str,
    sync::Arc,
};

use chrono::{prelude::*, Duration};
use chrono_tz::Tz;
use either::Either;
use uuid::Uuid;

use crate::{
    errors::{Error, FromSqlError, Result},
    types::{
        column::datetime64::to_datetime,
        decimal::Decimal,
        value::{decode_ipv4, decode_ipv6, AppDate, AppDateTime},
        DateTimeType, Enum16, Enum8, SqlType, Value,
    },
};

#[derive(Clone, Debug)]
pub enum ValueRef<'a> {
    Bool(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    String(&'a [u8]),
    Float32(f32),
    Float64(f64),
    Date(u16),
    DateTime(u32, Tz),
    DateTime64(i64, &'a (u32, Tz)),
    Nullable(Either<&'static SqlType, Box<ValueRef<'a>>>),
    Array(&'static SqlType, Arc<Vec<ValueRef<'a>>>),
    Decimal(Decimal),
    Ipv4([u8; 4]),
    Ipv6([u8; 16]),
    Uuid([u8; 16]),
    Enum16(Vec<(String, i16)>, Enum16),
    Enum8(Vec<(String, i8)>, Enum8),
    Map(
        &'static SqlType,
        &'static SqlType,
        Arc<HashMap<ValueRef<'a>, ValueRef<'a>>>,
    ),
}

impl<'a> Hash for ValueRef<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::String(s) => s.hash(state),
            Self::Int8(i) => i.hash(state),
            Self::Int16(i) => i.hash(state),
            Self::Int32(i) => i.hash(state),
            Self::Int64(i) => i.hash(state),
            Self::Int128(i) => i.hash(state),
            Self::UInt8(i) => i.hash(state),
            Self::UInt16(i) => i.hash(state),
            Self::UInt32(i) => i.hash(state),
            Self::UInt64(i) => i.hash(state),
            Self::UInt128(i) => i.hash(state),
            _ => unimplemented!(),
        }
    }
}

impl<'a> Eq for ValueRef<'a> {}

impl<'a> PartialEq for ValueRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueRef::UInt8(a), ValueRef::UInt8(b)) => *a == *b,
            (ValueRef::UInt16(a), ValueRef::UInt16(b)) => *a == *b,
            (ValueRef::UInt32(a), ValueRef::UInt32(b)) => *a == *b,
            (ValueRef::UInt64(a), ValueRef::UInt64(b)) => *a == *b,
            (ValueRef::UInt128(a), ValueRef::UInt128(b)) => *a == *b,
            (ValueRef::Int8(a), ValueRef::Int8(b)) => *a == *b,
            (ValueRef::Int16(a), ValueRef::Int16(b)) => *a == *b,
            (ValueRef::Int32(a), ValueRef::Int32(b)) => *a == *b,
            (ValueRef::Int64(a), ValueRef::Int64(b)) => *a == *b,
            (ValueRef::Int128(a), ValueRef::Int128(b)) => *a == *b,
            (ValueRef::String(a), ValueRef::String(b)) => *a == *b,
            (ValueRef::Float32(a), ValueRef::Float32(b)) => *a == *b,
            (ValueRef::Float64(a), ValueRef::Float64(b)) => *a == *b,
            (ValueRef::Date(a), ValueRef::Date(b)) => *a == *b,
            (ValueRef::DateTime(a, tz_a), ValueRef::DateTime(b, tz_b)) => {
                let time_a = tz_a.timestamp_opt(i64::from(*a), 0);
                let time_b = tz_b.timestamp_opt(i64::from(*b), 0);
                time_a == time_b
            }
            (ValueRef::Nullable(a), ValueRef::Nullable(b)) => *a == *b,
            (ValueRef::Array(ta, a), ValueRef::Array(tb, b)) => *ta == *tb && *a == *b,
            (ValueRef::Decimal(a), ValueRef::Decimal(b)) => *a == *b,
            (ValueRef::Enum8(a0, a1), ValueRef::Enum8(b0, b1)) => *a1 == *b1 && *a0 == *b0,
            (ValueRef::Enum16(a0, a1), ValueRef::Enum16(b0, b1)) => *a1 == *b1 && *a0 == *b0,
            (ValueRef::DateTime64(this, this_params), ValueRef::DateTime64(that, that_params)) => {
                let (this_precision, this_tz) = **this_params;
                let (that_precision2, that_tz) = **that_params;

                let this_time = to_datetime(*this, this_precision, this_tz);
                let that_time = to_datetime(*that, that_precision2, that_tz);

                this_time == that_time
            }
            (ValueRef::Map(a1, a2, map1), ValueRef::Map(b1, b2, map2)) => {
                if map1.len() != map2.len() || a1 != b1 || a2 != b2 {
                    return false;
                }
                map1 == map2
            }
            _ => false,
        }
    }
}

impl<'a> fmt::Display for ValueRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueRef::Bool(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt8(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt16(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt32(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt64(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt128(v) => fmt::Display::fmt(v, f),
            ValueRef::Int8(v) => fmt::Display::fmt(v, f),
            ValueRef::Int16(v) => fmt::Display::fmt(v, f),
            ValueRef::Int32(v) => fmt::Display::fmt(v, f),
            ValueRef::Int64(v) => fmt::Display::fmt(v, f),
            ValueRef::Int128(v) => fmt::Display::fmt(v, f),
            ValueRef::String(v) => match str::from_utf8(v) {
                Ok(s) => fmt::Display::fmt(s, f),
                Err(_) => write!(f, "{:?}", *v),
            },
            ValueRef::Float32(v) => fmt::Display::fmt(v, f),
            ValueRef::Float64(v) => fmt::Display::fmt(v, f),
            ValueRef::Date(v) if f.alternate() => {
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .map(|unix_epoch| unix_epoch + Duration::days((*v).into()))
                    .unwrap();
                fmt::Display::fmt(&date, f)
            }
            ValueRef::Date(v) => {
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .map(|unix_epoch| unix_epoch + Duration::days((*v).into()))
                    .unwrap();
                fmt::Display::fmt(&date.format("%Y-%m-%d"), f)
            }
            ValueRef::DateTime(u, tz) if f.alternate() => {
                let time = tz.timestamp_opt(i64::from(*u), 0).unwrap();
                write!(f, "{}", time.to_rfc2822())
            }
            ValueRef::DateTime(u, tz) => {
                let time = tz.timestamp_opt(i64::from(*u), 0).unwrap();
                fmt::Display::fmt(&time.format("%Y-%m-%d %H:%M:%S"), f)
            }
            ValueRef::DateTime64(u, params) => {
                let (precision, tz) = **params;
                let time = to_datetime(*u, precision, tz);
                fmt::Display::fmt(&time.format("%Y-%m-%d %H:%M:%S"), f)
            }
            ValueRef::Nullable(v) => match v {
                Either::Left(_) => write!(f, "NULL"),
                Either::Right(inner) => write!(f, "{inner}"),
            },
            ValueRef::Array(_, vs) => {
                let cells: Vec<String> = vs.iter().map(|v| format!("{v}")).collect();
                write!(f, "[{}]", cells.join(", "))
            }
            ValueRef::Decimal(v) => fmt::Display::fmt(v, f),
            ValueRef::Ipv4(v) => {
                write!(f, "{}", decode_ipv4(v))
            }
            ValueRef::Ipv6(v) => {
                write!(f, "{}", decode_ipv6(v))
            }
            ValueRef::Uuid(v) => {
                let mut buffer = *v;
                buffer[..8].reverse();
                buffer[8..].reverse();
                match Uuid::from_slice(&buffer) {
                    Ok(uuid) => write!(f, "{uuid}"),
                    Err(e) => write!(f, "{e}"),
                }
            }
            ValueRef::Enum8(_, v) => fmt::Display::fmt(v, f),
            ValueRef::Enum16(_, v) => fmt::Display::fmt(v, f),
            ValueRef::Map(_, _, vs) => {
                let cells: Vec<String> = vs.iter().map(|v| format!("{}-{}", v.0, v.1)).collect();
                write!(f, "[{}]", cells.join(", "))
            }
        }
    }
}

impl<'a> From<ValueRef<'a>> for SqlType {
    fn from(source: ValueRef<'a>) -> Self {
        match source {
            ValueRef::Bool(_) => SqlType::Bool,
            ValueRef::UInt8(_) => SqlType::UInt8,
            ValueRef::UInt16(_) => SqlType::UInt16,
            ValueRef::UInt32(_) => SqlType::UInt32,
            ValueRef::UInt64(_) => SqlType::UInt64,
            ValueRef::UInt128(_) => SqlType::UInt128,
            ValueRef::Int8(_) => SqlType::Int8,
            ValueRef::Int16(_) => SqlType::Int16,
            ValueRef::Int32(_) => SqlType::Int32,
            ValueRef::Int64(_) => SqlType::Int64,
            ValueRef::Int128(_) => SqlType::Int128,
            ValueRef::String(_) => SqlType::String,
            ValueRef::Float32(_) => SqlType::Float32,
            ValueRef::Float64(_) => SqlType::Float64,
            ValueRef::Date(_) => SqlType::Date,
            ValueRef::DateTime(_, _) => SqlType::DateTime(DateTimeType::DateTime32),
            ValueRef::Nullable(u) => match u {
                Either::Left(sql_type) => SqlType::Nullable(sql_type),
                Either::Right(value_ref) => SqlType::Nullable(SqlType::from(*value_ref).into()),
            },
            ValueRef::Array(t, _) => SqlType::Array(t),
            ValueRef::Decimal(v) => SqlType::Decimal(v.precision, v.scale),
            ValueRef::Enum8(values, _) => SqlType::Enum8(values),
            ValueRef::Enum16(values, _) => SqlType::Enum16(values),
            ValueRef::Ipv4(_) => SqlType::Ipv4,
            ValueRef::Ipv6(_) => SqlType::Ipv6,
            ValueRef::Uuid(_) => SqlType::Uuid,
            ValueRef::DateTime64(_, params) => {
                let (precision, tz) = params;
                SqlType::DateTime(DateTimeType::DateTime64(*precision, *tz))
            }
            ValueRef::Map(k, v, _) => SqlType::Map(k, v),
        }
    }
}

impl<'a> ValueRef<'a> {
    pub fn as_str(&self) -> Result<&'a str> {
        if let ValueRef::String(t) = self {
            return Ok(str::from_utf8(t)?);
        }
        let from = SqlType::from(self.clone()).to_string();
        Err(Error::FromSql(FromSqlError::InvalidType {
            src: from,
            dst: "&str".into(),
        }))
    }

    pub fn as_string(&self) -> Result<String> {
        let tmp = self.as_str()?;
        Ok(tmp.to_string())
    }

    pub fn as_bytes(&self) -> Result<&'a [u8]> {
        if let ValueRef::String(t) = self {
            return Ok(t);
        }
        let from = SqlType::from(self.clone()).to_string();
        Err(Error::FromSql(FromSqlError::InvalidType {
            src: from,
            dst: "&[u8]".into(),
        }))
    }
}

impl<'a> From<ValueRef<'a>> for Value {
    fn from(borrowed: ValueRef<'a>) -> Self {
        match borrowed {
            ValueRef::Bool(v) => Value::Bool(v),
            ValueRef::UInt8(v) => Value::UInt8(v),
            ValueRef::UInt16(v) => Value::UInt16(v),
            ValueRef::UInt32(v) => Value::UInt32(v),
            ValueRef::UInt64(v) => Value::UInt64(v),
            ValueRef::UInt128(v) => Value::UInt128(v),
            ValueRef::Int8(v) => Value::Int8(v),
            ValueRef::Int16(v) => Value::Int16(v),
            ValueRef::Int32(v) => Value::Int32(v),
            ValueRef::Int64(v) => Value::Int64(v),
            ValueRef::Int128(v) => Value::Int128(v),
            ValueRef::String(v) => Value::String(Arc::new(v.into())),
            ValueRef::Float32(v) => Value::Float32(v),
            ValueRef::Float64(v) => Value::Float64(v),
            ValueRef::Date(v) => Value::Date(v),
            ValueRef::DateTime(v, tz) => Value::DateTime(v, tz),
            ValueRef::Nullable(u) => match u {
                Either::Left(sql_type) => Value::Nullable(Either::Left((sql_type.clone()).into())),
                Either::Right(v) => {
                    let value: Value = (*v).into();
                    Value::Nullable(Either::Right(Box::new(value)))
                }
            },
            ValueRef::Array(t, vs) => {
                let mut value_list: Vec<Value> = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    let value: Value = v.clone().into();
                    value_list.push(value);
                }
                Value::Array(t, Arc::new(value_list))
            }
            ValueRef::Decimal(v) => Value::Decimal(v),
            ValueRef::Enum8(e_v, v) => Value::Enum8(e_v, v),
            ValueRef::Enum16(e_v, v) => Value::Enum16(e_v, v),
            ValueRef::Ipv4(v) => Value::Ipv4(v),
            ValueRef::Ipv6(v) => Value::Ipv6(v),
            ValueRef::Uuid(v) => Value::Uuid(v),
            ValueRef::DateTime64(v, params) => Value::DateTime64(v, *params),
            ValueRef::Map(k, v, vs) => {
                let mut value_list: HashMap<Value, Value> = HashMap::with_capacity(vs.len());
                for (k, v) in vs.iter() {
                    let key: Value = k.clone().into();
                    let value: Value = v.clone().into();
                    value_list.insert(key, value);
                }
                Value::Map(k, v, Arc::new(value_list))
            }
        }
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(s: &str) -> ValueRef {
        ValueRef::String(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(bs: &[u8]) -> ValueRef {
        ValueRef::String(bs)
    }
}

macro_rules! from_number {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl<'a> From<$t> for ValueRef<'a> {
                fn from(v: $t) -> ValueRef<'static> {
                    ValueRef::$k(v)
                }
            }
        )*
    };
}

from_number! {
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

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> ValueRef<'a> {
        match value {
            Value::Bool(v) => ValueRef::Bool(*v),
            Value::UInt8(v) => ValueRef::UInt8(*v),
            Value::UInt16(v) => ValueRef::UInt16(*v),
            Value::UInt32(v) => ValueRef::UInt32(*v),
            Value::UInt64(v) => ValueRef::UInt64(*v),
            Value::UInt128(v) => ValueRef::UInt128(*v),
            Value::Int8(v) => ValueRef::Int8(*v),
            Value::Int16(v) => ValueRef::Int16(*v),
            Value::Int32(v) => ValueRef::Int32(*v),
            Value::Int64(v) => ValueRef::Int64(*v),
            Value::Int128(v) => ValueRef::Int128(*v),
            Value::String(v) => ValueRef::String(v),
            Value::Float32(v) => ValueRef::Float32(*v),
            Value::Float64(v) => ValueRef::Float64(*v),
            Value::Date(v) => ValueRef::Date(*v),
            Value::DateTime(v, tz) => ValueRef::DateTime(*v, *tz),
            Value::DateTime64(v, params) => ValueRef::DateTime64(*v, params),
            Value::Nullable(u) => match u {
                Either::Left(sql_type) => ValueRef::Nullable(Either::Left(sql_type.to_owned())),
                Either::Right(v) => {
                    let value_ref = v.as_ref().into();
                    ValueRef::Nullable(Either::Right(Box::new(value_ref)))
                }
            },
            Value::Array(t, vs) => {
                let mut ref_vec = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    let value_ref: ValueRef<'a> = From::from(v);
                    ref_vec.push(value_ref)
                }
                ValueRef::Array(t, Arc::new(ref_vec))
            }
            Value::Decimal(v) => ValueRef::Decimal(v.clone()),
            Value::Enum8(values, v) => ValueRef::Enum8(values.to_vec(), *v),
            Value::Enum16(values, v) => ValueRef::Enum16(values.to_vec(), *v),
            Value::Ipv4(v) => ValueRef::Ipv4(*v),
            Value::Ipv6(v) => ValueRef::Ipv6(*v),
            Value::Uuid(v) => ValueRef::Uuid(*v),
            Value::ChronoDateTime(_) => unimplemented!(),
            Value::Map(k, v, vs) => {
                let mut ref_map = HashMap::with_capacity(vs.len());
                for (k, v) in vs.iter() {
                    let key_ref: ValueRef<'a> = From::from(k);
                    let value_ref: ValueRef<'a> = From::from(v);
                    ref_map.insert(key_ref, value_ref);
                }
                ValueRef::Map(k, v, Arc::new(ref_map))
            }
        }
    }
}

macro_rules! value_from {
    ( $( $t:ty: $k:ident ),* ) => {
        $(
            impl<'a> From<ValueRef<'a>> for $t {
                fn from(value: ValueRef<'a>) -> Self {
                    if let ValueRef::$k(v) = value {
                        return v
                    }
                    let from = format!("{}", SqlType::from(value.clone()));
                    panic!("Can't convert ValueRef::{} into {}.",
                            from, stringify!($t))
                }
            }
        )*
    };
}

impl<'a> From<ValueRef<'a>> for AppDate {
    fn from(value: ValueRef<'a>) -> Self {
        if let ValueRef::Date(v) = value {
            return NaiveDate::from_ymd_opt(1970, 1, 1)
                .map(|unix_epoch| unix_epoch + Duration::days(v.into()))
                .unwrap();
        }
        let from = format!("{}", SqlType::from(value.clone()));
        panic!("Can't convert ValueRef::{} into {}.", from, stringify!($t))
    }
}

impl<'a> From<ValueRef<'a>> for Enum8 {
    fn from(value: ValueRef<'a>) -> Self {
        if let ValueRef::Enum8(_, b) = value {
            return b;
        }
        let from = format!("{}", SqlType::from(value.clone()));
        panic!("Can't convert ValueRef::{} into {}.", from, stringify!($t))
    }
}

impl<'a> From<ValueRef<'a>> for Enum16 {
    fn from(value: ValueRef<'a>) -> Self {
        if let ValueRef::Enum16(_, b) = value {
            return b;
        }
        let from = format!("{}", SqlType::from(value.clone()));
        panic!("Can't convert ValueRef::{} into {}.", from, stringify!($t))
    }
}

impl<'a> From<ValueRef<'a>> for AppDateTime {
    fn from(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::DateTime(x, tz) => tz.timestamp_opt(i64::from(x), 0).unwrap(),
            ValueRef::DateTime64(x, params) => {
                let (precision, tz) = *params;
                to_datetime(x, precision, tz)
            }
            _ => {
                let from = format!("{}", SqlType::from(value.clone()));
                panic!("Can't convert ValueRef::{} into {}.", from, "DateTime<Tz>")
            }
        }
    }
}

value_from! {
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
    use super::*;
    use crate::types::column::datetime64::DEFAULT_TZ;

    #[test]
    fn test_display() {
        assert_eq!(
            "[0, 159, 146, 150]".to_string(),
            format!("{}", ValueRef::String(&[0, 159, 146, 150]))
        );

        assert_eq!("text".to_string(), format!("{}", ValueRef::String(b"text")));

        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt8(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt16(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt32(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt64(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt128(42)));

        assert_eq!("42".to_string(), format!("{}", ValueRef::Int8(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Int16(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Int32(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Int64(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Int128(42)));

        assert_eq!("42".to_string(), format!("{}", ValueRef::Float32(42.0)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Float64(42.0)));

        assert_eq!(
            "NULL".to_string(),
            format!(
                "{}",
                ValueRef::Nullable(Either::Left(SqlType::UInt8.into()))
            )
        );

        assert_eq!(
            "42".to_string(),
            format!(
                "{}",
                ValueRef::Nullable(Either::Right(Box::new(ValueRef::UInt8(42))))
            )
        );

        assert_eq!(
            "[1, 2, 3]".to_string(),
            format!(
                "{}",
                ValueRef::Array(
                    SqlType::Int32.into(),
                    Arc::new(vec![
                        ValueRef::Int32(1),
                        ValueRef::Int32(2),
                        ValueRef::Int32(3)
                    ])
                )
            )
        );

        assert_eq!("1970-01-01".to_string(), format!("{}", ValueRef::Date(0)));

        assert_eq!("1970-01-01".to_string(), format!("{:#}", ValueRef::Date(0)));

        assert_eq!(
            "1970-01-01 00:00:00".to_string(),
            format!("{}", ValueRef::DateTime(0, *DEFAULT_TZ))
        );

        assert!(
            (format!("{:#}", ValueRef::DateTime(0, *DEFAULT_TZ))
                == "Thu, 1 Jan 1970 00:00:00 +0000")
                || (format!("{:#}", ValueRef::DateTime(0, *DEFAULT_TZ))
                    == "Thu, 01 Jan 1970 00:00:00 +0000")
        );

        assert_eq!(
            "2.00".to_string(),
            format!("{}", ValueRef::Decimal(Decimal::of(2.0_f64, 2)))
        )
    }

    #[test]
    fn test_size_of() {
        use std::mem;
        assert_eq!(32, mem::size_of::<[ValueRef<'_>; 1]>());
    }

    #[test]
    fn test_value_from_ref() {
        assert_eq!(Value::from(ValueRef::UInt8(42)), Value::UInt8(42));
        assert_eq!(Value::from(ValueRef::UInt16(42)), Value::UInt16(42));
        assert_eq!(Value::from(ValueRef::UInt32(42)), Value::UInt32(42));
        assert_eq!(Value::from(ValueRef::UInt64(42)), Value::UInt64(42));
        assert_eq!(Value::from(ValueRef::UInt128(42)), Value::UInt128(42));

        assert_eq!(Value::from(ValueRef::Int8(42)), Value::Int8(42));
        assert_eq!(Value::from(ValueRef::Int16(42)), Value::Int16(42));
        assert_eq!(Value::from(ValueRef::Int32(42)), Value::Int32(42));
        assert_eq!(Value::from(ValueRef::Int64(42)), Value::Int64(42));
        assert_eq!(Value::from(ValueRef::Int128(42)), Value::Int128(42));

        assert_eq!(Value::from(ValueRef::Float32(42.0)), Value::Float32(42.0));
        assert_eq!(Value::from(ValueRef::Float64(42.0)), Value::Float64(42.0));

        assert_eq!(Value::from(ValueRef::Date(42)), Value::Date(42));
        assert_eq!(
            Value::from(ValueRef::DateTime(42, *DEFAULT_TZ)),
            Value::DateTime(42, *DEFAULT_TZ)
        );

        assert_eq!(
            Value::from(ValueRef::Decimal(Decimal::of(2.0_f64, 4))),
            Value::Decimal(Decimal::of(2.0_f64, 4))
        );

        assert_eq!(
            Value::from(ValueRef::Array(
                SqlType::Int32.into(),
                Arc::new(vec![
                    ValueRef::Int32(1),
                    ValueRef::Int32(2),
                    ValueRef::Int32(3)
                ])
            )),
            Value::Array(
                SqlType::Int32.into(),
                Arc::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
            )
        )
    }

    #[test]
    fn test_uuid() {
        let uuid = Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap();
        let mut buffer = *uuid.as_bytes();
        buffer[..8].reverse();
        buffer[8..].reverse();
        let v = ValueRef::Uuid(buffer);
        assert_eq!(v.to_string(), "936da01f-9abd-4d9d-80c7-02af85c822a8");
    }

    #[test]
    fn test_get_sql_type() {
        assert_eq!(SqlType::from(ValueRef::UInt8(42)), SqlType::UInt8);
        assert_eq!(SqlType::from(ValueRef::UInt16(42)), SqlType::UInt16);
        assert_eq!(SqlType::from(ValueRef::UInt32(42)), SqlType::UInt32);
        assert_eq!(SqlType::from(ValueRef::UInt64(42)), SqlType::UInt64);
        assert_eq!(SqlType::from(ValueRef::UInt128(42)), SqlType::UInt128);

        assert_eq!(SqlType::from(ValueRef::Int8(42)), SqlType::Int8);
        assert_eq!(SqlType::from(ValueRef::Int16(42)), SqlType::Int16);
        assert_eq!(SqlType::from(ValueRef::Int32(42)), SqlType::Int32);
        assert_eq!(SqlType::from(ValueRef::Int64(42)), SqlType::Int64);
        assert_eq!(SqlType::from(ValueRef::Int128(42)), SqlType::Int128);

        assert_eq!(SqlType::from(ValueRef::Float32(42.0)), SqlType::Float32);
        assert_eq!(SqlType::from(ValueRef::Float64(42.0)), SqlType::Float64);

        assert_eq!(SqlType::from(ValueRef::String(&[])), SqlType::String);

        assert_eq!(SqlType::from(ValueRef::Date(42)), SqlType::Date);
        assert_eq!(
            SqlType::from(ValueRef::DateTime(42, *DEFAULT_TZ)),
            SqlType::DateTime(DateTimeType::DateTime32)
        );

        assert_eq!(
            SqlType::from(ValueRef::Decimal(Decimal::of(2.0_f64, 4))),
            SqlType::Decimal(38, 4)
        );

        assert_eq!(
            SqlType::from(ValueRef::Array(
                SqlType::Int32.into(),
                Arc::new(vec![
                    ValueRef::Int32(1),
                    ValueRef::Int32(2),
                    ValueRef::Int32(3)
                ])
            )),
            SqlType::Array(SqlType::Int32.into())
        );

        assert_eq!(
            SqlType::from(ValueRef::Nullable(Either::Left(SqlType::UInt8.into()))),
            SqlType::Nullable(SqlType::UInt8.into())
        );

        assert_eq!(
            SqlType::from(ValueRef::Nullable(Either::Right(Box::new(ValueRef::Int8(
                42
            ))))),
            SqlType::Nullable(SqlType::Int8.into())
        );
    }
}
