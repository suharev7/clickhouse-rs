extern crate regex;

use chrono_tz::Tz;

use regex::Regex;

use crate::{
    binary::ReadEx,
    errors::Result,
    types::column::{
        array::ArrayColumnData, column_data::ColumnData, date::DateColumnData,
        decimal::DecimalColumnData, fixed_string::FixedStringColumnData, list::List,
        nullable::NullableColumnData, numeric::VectorColumnData, string::StringColumnData,
        BoxColumnWrapper, ColumnWrapper, enums::Enum8ColumnData,
        ip::{IpColumnData, Ipv4, Ipv6, Uuid},
        BoxColumnWrapper, ColumnWrapper,enums::{Enum8ColumnData},
    },
    types::decimal::NoBits,
    SqlType,
};
use crate::types::enums::EnumSize;

macro_rules! match_str {
    ($arg:ident, {
        $( $($var:literal)|* => $doit:expr,)*
        _ => $dothat:block
    }) => {
        $(
            $(
                if $arg.eq_ignore_ascii_case($var) {
                    $doit
                } else
            )*
        )*
        $dothat
    }
}

impl dyn ColumnData {
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn load_data<W: ColumnWrapper, T: ReadEx>(
        reader: &mut T,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<W::Wrapper> {
        Ok(match_str!(type_name, {
            "UInt8" => W::wrap(VectorColumnData::<u8>::load(reader, size)?),
            "UInt16" => W::wrap(VectorColumnData::<u16>::load(reader, size)?),
            "UInt32" => W::wrap(VectorColumnData::<u32>::load(reader, size)?),
            "UInt64" => W::wrap(VectorColumnData::<u64>::load(reader, size)?),
            "Int8" | "TinyInt" => W::wrap(VectorColumnData::<i8>::load(reader, size)?),
            "Int16" | "SmallInt" => W::wrap(VectorColumnData::<i16>::load(reader, size)?),
            "Int32" | "Int" | "Integer" => W::wrap(VectorColumnData::<i32>::load(reader, size)?),
            "Int64" | "BigInt" => W::wrap(VectorColumnData::<i64>::load(reader, size)?),
            "Float32" | "Float" => W::wrap(VectorColumnData::<f32>::load(reader, size)?),
            "Float64" | "Double" => W::wrap(VectorColumnData::<f64>::load(reader, size)?),
            "String" | "Char" | "Varchar" | "Text" | "TinyText" | "MediumText" | "LongText" | "Blob" | "TinyBlob" | "MediumBlob" | "LongBlob" => W::wrap(StringColumnData::load(reader, size)?),
            "Date" => W::wrap(DateColumnData::<u16>::load(reader, size, tz)?),
            "DateTime" | "Timestamp" => W::wrap(DateColumnData::<u32>::load(reader, size, tz)?),
            "IPv4" => W::wrap(IpColumnData::<Ipv4>::load(reader, size)?),
            "IPv6" => W::wrap(IpColumnData::<Ipv6>::load(reader, size)?),
            "UUID" => W::wrap(IpColumnData::<Uuid>::load(reader, size)?),
            _ => {
                if let Some(inner_type) = parse_nullable_type(type_name) {
                    W::wrap(NullableColumnData::load(reader, inner_type, size, tz)?)
                } else if let Some(str_len) = parse_fixed_string(type_name) {
                    W::wrap(FixedStringColumnData::load(reader, size, str_len)?)
                } else if let Some(inner_type) = parse_array_type(type_name) {
                    W::wrap(ArrayColumnData::load(reader, inner_type, size, tz)?)
                } else if let Some((precision, scale, nobits)) = parse_decimal(type_name) {
                    W::wrap(DecimalColumnData::load(
                        reader, precision, scale, nobits, size, tz,
                    )?)
                } else if let Some((enum_size, items)) = parse_enum(type_name) {
                    W::wrap(EnumColumnData::load(reader, enum_size, items, size, tz)?)
                } else {
                    let message = format!("Unsupported column type \"{}\".", type_name);
                    return Err(message.into());
                }
            }
        }))
    }

    pub(crate) fn from_type<W: ColumnWrapper>(
        sql_type: SqlType,
        timezone: Tz,
        capacity: usize,
    ) -> Result<W::Wrapper> {
        Ok(match sql_type {
            SqlType::UInt8 => W::wrap(VectorColumnData::<u8>::with_capacity(capacity)),
            SqlType::UInt16 => W::wrap(VectorColumnData::<u16>::with_capacity(capacity)),
            SqlType::UInt32 => W::wrap(VectorColumnData::<u32>::with_capacity(capacity)),
            SqlType::UInt64 => W::wrap(VectorColumnData::<u64>::with_capacity(capacity)),
            SqlType::Int8 => W::wrap(VectorColumnData::<i8>::with_capacity(capacity)),
            SqlType::Int16 => W::wrap(VectorColumnData::<i16>::with_capacity(capacity)),
            SqlType::Int32 => W::wrap(VectorColumnData::<i32>::with_capacity(capacity)),
            SqlType::Int64 => W::wrap(VectorColumnData::<i64>::with_capacity(capacity)),
            SqlType::String => W::wrap(StringColumnData::with_capacity(capacity)),
            SqlType::FixedString(len) => {
                W::wrap(FixedStringColumnData::with_capacity(capacity, len))
            }
            SqlType::Float32 => W::wrap(VectorColumnData::<f32>::with_capacity(capacity)),
            SqlType::Float64 => W::wrap(VectorColumnData::<f64>::with_capacity(capacity)),

            SqlType::Ipv4 => W::wrap(IpColumnData::<Ipv4>::with_capacity(capacity)),
            SqlType::Ipv6 => W::wrap(IpColumnData::<Ipv6>::with_capacity(capacity)),
            SqlType::Uuid => W::wrap(IpColumnData::<Uuid>::with_capacity(capacity)),

            SqlType::Date => W::wrap(DateColumnData::<u16>::with_capacity(capacity, timezone)),
            SqlType::DateTime => W::wrap(DateColumnData::<u32>::with_capacity(capacity, timezone)),
            SqlType::Nullable(inner_type) => W::wrap(NullableColumnData {
                inner: ColumnData::from_type::<BoxColumnWrapper>(*inner_type, timezone, capacity)?,
                nulls: Vec::new(),
            }),
            SqlType::Array(inner_type) => W::wrap(ArrayColumnData {
                inner: ColumnData::from_type::<BoxColumnWrapper>(*inner_type, timezone, capacity)?,
                offsets: List::with_capacity(capacity),
            }),
            SqlType::Decimal(precision, scale) => {
                let nobits = NoBits::from_precision(precision).unwrap();

                let inner_type = match nobits {
                    NoBits::N32 => SqlType::Int32,
                    NoBits::N64 => SqlType::Int64,
                };

                W::wrap(DecimalColumnData {
                    inner: ColumnData::from_type::<BoxColumnWrapper>(
                        inner_type, timezone, capacity,
                    )?,
                    precision,
                    scale,
                    nobits,
                })
            }
<<<<<<< HEAD
            SqlType::Enum8(values) => W::wrap(VectorColumnData::<i8>::with_capacity(DEFAULT_CAPACITY)),
            SqlType::Enum16 => W::wrap(VectorColumnData::<i16>::with_capacity(DEFAULT_CAPACITY)),
=======
            SqlType::Enum(size, enum_values) => {
                let inner_type = match size {
                    EnumSize::ENUM8 => SqlType::Int8,
                    EnumSize::ENUM16 => SqlType::Int16,
                };

                W::wrap(EnumColumnData {
                    enum_values,
                    size,
                    inner: ColumnData::from_type::<BoxColumnWrapper>(inner_type, timezone)?,
                })
            }
>>>>>>> c74dcbf... Work with enum8 and enum16
        })
    }
}

fn parse_fixed_string(source: &str) -> Option<usize> {
    if !source.starts_with("FixedString") {
        return None;
    }

    let inner_size = &source[12..source.len() - 1];
    match inner_size.parse::<usize>() {
        Err(_) => None,
        Ok(value) => Some(value),
    }
}

fn parse_nullable_type(source: &str) -> Option<&str> {
    if !source.starts_with("Nullable") {
        return None;
    }

    let inner_type = &source[9..source.len() - 1];

    if inner_type.starts_with("Nullable") {
        return None;
    }

    Some(inner_type)
}

fn parse_array_type(source: &str) -> Option<&str> {
    if !source.starts_with("Array") {
        return None;
    }

    let inner_type = &source[6..source.len() - 1];
    Some(inner_type)
}

fn parse_decimal(source: &str) -> Option<(u8, u8, NoBits)> {
    if source.len() < 12 {
        return None;
    }

    if !source.starts_with("Decimal") {
        return None;
    }

    let mut nobits = None;
    let mut precision = None;
    let mut scale = None;

    let mut params_indexes = (None, None);

    for (idx, byte) in source.as_bytes().iter().enumerate() {
        if *byte == b'(' {
            match &source.as_bytes()[..idx] {
                b"Decimal" => {}
                b"Decimal32" => {
                    nobits = Some(NoBits::N32);
                }
                b"Decimal64" => {
                    nobits = Some(NoBits::N64);
                }
                _ => return None,
            }
            params_indexes.0 = Some(idx);
        }
        if *byte == b')' {
            params_indexes.1 = Some(idx);
        }
    }

    let params_indexes = match params_indexes {
        (Some(start), Some(end)) => (start, end),
        _ => return None,
    };

    match nobits {
        Some(_) => {
            scale = std::str::from_utf8(&source.as_bytes()[params_indexes.0 + 1..params_indexes.1])
                .unwrap()
                .parse()
                .ok()
        }
        None => {
            for (idx, cell) in
                std::str::from_utf8(&source.as_bytes()[params_indexes.0 + 1..params_indexes.1])
                    .unwrap()
                    .split(',')
                    .map(|s| s.trim())
                    .enumerate()
            {
                match idx {
                    0 => precision = cell.parse().ok(),
                    1 => scale = cell.parse().ok(),
                    _ => return None,
                }
            }
        }
    }

    match (precision, scale, nobits) {
        (Some(precision), Some(scale), None) => {
            if scale > precision {
                return None;
            }

            if let Some(nobits) = NoBits::from_precision(precision) {
                Some((precision, scale, nobits))
            } else {
                None
            }
        }
        (None, Some(scale), Some(bits)) => {
            let precision = match bits {
                NoBits::N32 => 9,
                NoBits::N64 => 18,
            };
            Some((precision, scale, bits))
        }
        _ => None,
    }
}

fn parse_enum(source: &str) -> Option<(EnumSize, Vec<(String, i16)>)> {
    let re_enum = Regex::new(r"(?P<values>Enum(?P<enum_size>\d{1,3})\(([a-zA-Z' =\-\d,]*\))+)").unwrap();
    let re_values = Regex::new(r"(?P<name>[^']+)' = (?P<value>\-?\d{1,4})").unwrap();

    let mut real_enums = vec![];
    let mut size = EnumSize::ENUM8;
    for cap in re_enum.captures_iter(source) {
        size = match &cap["enum_size"] {
            "8" => EnumSize::ENUM8,
            "16" => EnumSize::ENUM16,
            _ => return None
        };

        for values in re_values.captures_iter(&cap[0]) {
            real_enums.push((values["name"].to_string(), values["value"].parse::<i16>().unwrap()));
        }
        break;
    };
    if real_enums.is_empty() {
        return None;
    }
    Some((size, real_enums))
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_decimal() {
        assert_eq!(parse_decimal("Decimal(9, 4)"), Some((9, 4, NoBits::N32)));
        assert_eq!(parse_decimal("Decimal(10, 4)"), Some((10, 4, NoBits::N64)));
        assert_eq!(parse_decimal("Decimal(20, 4)"), None);
        assert_eq!(parse_decimal("Decimal(2000, 4)"), None);
        assert_eq!(parse_decimal("Decimal(3, 4)"), None);
        assert_eq!(parse_decimal("Decimal(20, -4)"), None);
        assert_eq!(parse_decimal("Decimal(0)"), None);
        assert_eq!(parse_decimal("Decimal(1, 2, 3)"), None);
        assert_eq!(parse_decimal("Decimal64(9)"), Some((18, 9, NoBits::N64)));
    }

    #[test]
    fn test_parse_array_type() {
        assert_eq!(parse_array_type("Array(UInt8)"), Some("UInt8"));
    }

    #[test]
    fn test_parse_nullable_type() {
        assert_eq!(parse_nullable_type("Nullable(Int8)"), Some("Int8"));
        assert_eq!(parse_nullable_type("Int8"), None);
        assert_eq!(parse_nullable_type("Nullable(Nullable(Int8))"), None);
    }

    #[test]
    fn test_parse_fixed_string() {
        assert_eq!(parse_fixed_string("FixedString(8)"), Some(8_usize));
        assert_eq!(parse_fixed_string("FixedString(zz)"), None);
        assert_eq!(parse_fixed_string("Int8"), None);
    }
}
