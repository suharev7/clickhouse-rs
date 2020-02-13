use std::str::FromStr;

use chrono_tz::Tz;
use combine::{
    between,
    parser::{
        char::{alpha_num, digit, string},
        range::recognize,
    },
    sep_by1, skip_many, skip_many1, token, Parser,
};

use crate::{
    binary::ReadEx,
    errors::Result,
    types::column::{
        array::ArrayColumnData,
        column_data::ColumnData,
        date::DateColumnData,
        decimal::DecimalColumnData,
        enums::{Enum16ColumnData, Enum8ColumnData},
        fixed_string::FixedStringColumnData,
        ip::{IpColumnData, Ipv4, Ipv6, Uuid},
        list::List,
        nullable::NullableColumnData,
        numeric::VectorColumnData,
        string::StringColumnData,
        BoxColumnWrapper, ColumnWrapper,
    },
    types::decimal::NoBits,
    SqlType,
};

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
            "Int8" => W::wrap(VectorColumnData::<i8>::load(reader, size)?),
            "Int16" => W::wrap(VectorColumnData::<i16>::load(reader, size)?),
            "Int32" => W::wrap(VectorColumnData::<i32>::load(reader, size)?),
            "Int64" => W::wrap(VectorColumnData::<i64>::load(reader, size)?),
            "Float32" => W::wrap(VectorColumnData::<f32>::load(reader, size)?),
            "Float64" => W::wrap(VectorColumnData::<f64>::load(reader, size)?),
            "String" => W::wrap(StringColumnData::load(reader, size)?),
            "Date" => W::wrap(DateColumnData::<u16>::load(reader, size, tz)?),
            "DateTime" => W::wrap(DateColumnData::<u32>::load(reader, size, tz)?),
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
                } else if let Some(items) = parse_enum8(remove_white_spaces(type_name).as_str()) {
                    W::wrap(Enum8ColumnData::load(reader, items, size, tz)?)
                } else if let Some(items) = parse_enum16(remove_white_spaces(type_name).as_str()) {
                    W::wrap(Enum16ColumnData::load(reader, items, size, tz)?)
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
                inner: ColumnData::from_type::<BoxColumnWrapper>(
                    inner_type.clone(),
                    timezone,
                    capacity,
                )?,
                nulls: Vec::new(),
            }),
            SqlType::Array(inner_type) => W::wrap(ArrayColumnData {
                inner: ColumnData::from_type::<BoxColumnWrapper>(
                    inner_type.clone(),
                    timezone,
                    capacity,
                )?,
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
            SqlType::Enum8(enum_values) => W::wrap(Enum8ColumnData {
                enum_values,
                inner: ColumnData::from_type::<BoxColumnWrapper>(
                    SqlType::Int8,
                    timezone,
                    capacity,
                )?,
            }),
            SqlType::Enum16(enum_values) => W::wrap(Enum16ColumnData {
                enum_values,
                inner: ColumnData::from_type::<BoxColumnWrapper>(
                    SqlType::Int8,
                    timezone,
                    capacity,
                )?,
            }),
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

    if source.chars().nth(7) != Some('(') {
        return None;
    }

    if !source.ends_with(')') {
        return None;
    }

    let mut params: Vec<u8> = Vec::with_capacity(2);
    for cell in source[8..source.len() - 1].split(',').map(|s| s.trim()) {
        if let Ok(value) = cell.parse() {
            params.push(value)
        } else {
            return None;
        }
    }

    if params.len() != 2 {
        return None;
    }

    let precision = params[0];
    let scale = params[1];

    if scale > precision {
        return None;
    }

    if let Some(nobits) = NoBits::from_precision(precision) {
        return Some((precision, scale, nobits));
    }

    None
}

fn remove_white_spaces(input: &str) -> String {
    let input: String = input.split_whitespace().collect();
    input
}

enum EnumSize {
    Enum8,
    Enum16,
}

fn parse_enum8(input: &str) -> Option<Vec<(String, i8)>> {
    match parse_enum(EnumSize::Enum8, input) {
        Some(result) => {
            let res: Vec<(String, i8)> = result
                .iter()
                .map(|(key, val)| {
                    (
                        key.to_string(),
                        i8::from_str(val).expect("In Enum8 should used i8 values"),
                    )
                })
                .collect();
            return Some(res);
        }
        None => None,
    }
}
fn parse_enum16(input: &str) -> Option<Vec<(String, i16)>> {
    match parse_enum(EnumSize::Enum16, input) {
        Some(result) => {
            let res: Vec<(String, i16)> = result
                .iter()
                .map(|(key, val)| {
                    (
                        key.to_string(),
                        i16::from_str(val).expect("In Enum16 should used i16 values"),
                    )
                })
                .collect();
            return Some(res);
        }
        None => None,
    }
}

fn parse_enum(size: EnumSize, input: &str) -> Option<Vec<(&str, &str)>> {
    let size = match size {
        EnumSize::Enum8 => "Enum8",
        EnumSize::Enum16 => "Enum16",
    };
    let identifier = between(token('\''), token('\''), recognize(skip_many(alpha_num())));
    let value = recognize(skip_many1(digit()));
    let variant = identifier.skip(token('=')).and(value);
    let parens = between(token('('), token(')'), sep_by1(variant, token(',')));
    let mut tag = string(size).with(parens);
    let result = tag.parse(input).map(|x| x.0);

    result.ok()
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

    #[test]
    fn test_parse_enum8() {
        let enum8 = remove_white_spaces("Enum8 ('a' = 1, 'b' = 2)");

        let res = parse_enum8(enum8.as_str()).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1), ("b".to_owned(), 2)])
    }

    #[test]
    fn test_parse_enum8_single() {
        let enum8 = remove_white_spaces("Enum8 ('a' = 1)");

        let res = parse_enum8(enum8.as_str()).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum8_empty_id() {
        let enum8 = remove_white_spaces("Enum8 ('' = 1, '' = 2)");

        let res = parse_enum8(enum8.as_str()).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1), ("".to_owned(), 2)])
    }

    #[test]
    fn test_parse_enum8_single_empty_id() {
        let enum8 = remove_white_spaces("Enum8 ('' = 1)");

        let res = parse_enum8(enum8.as_str()).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum8_extra_comma() {
        let enum8 = remove_white_spaces("Enum8 ('a' = 1, 'b' = 2,)");

        assert!(dbg!(parse_enum8(enum8.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum8_empty() {
        let enum8 = remove_white_spaces("Enum8 ()");

        assert!(dbg!(parse_enum8(enum8.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum8_no_value() {
        let enum8 = remove_white_spaces("Enum8 ('a' =)");

        assert!(dbg!(parse_enum8(enum8.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum8_no_ident() {
        let enum8 = remove_white_spaces("Enum8 ( = 1)");

        assert!(dbg!(parse_enum8(enum8.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum8_starting_comma() {
        let enum8 = remove_white_spaces("Enum8 ( , 'a' = 1)");

        assert!(dbg!(parse_enum8(enum8.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum16() {
        let enum16 = remove_white_spaces("Enum16 ('a' = 1, 'b' = 2)");

        let res = parse_enum16(enum16.as_str()).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1), ("b".to_owned(), 2)])
    }

    #[test]
    fn test_parse_enum16_single() {
        let enum16 = remove_white_spaces("Enum16 ('a' = 1)");

        let res = parse_enum16(enum16.as_str()).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum16_empty_id() {
        let enum16 = remove_white_spaces("Enum16 ('' = 1, '' = 2)");

        let res = parse_enum16(enum16.as_str()).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1), ("".to_owned(), 2)])
    }

    #[test]
    fn test_parse_enum16_single_empty_id() {
        let enum16 = remove_white_spaces("Enum16 ('' = 1)");

        let res = parse_enum16(enum16.as_str()).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum16_extra_comma() {
        let enum16 = remove_white_spaces("Enum16 ('a' = 1, 'b' = 2,)");

        assert!(dbg!(parse_enum16(enum16.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum16_empty() {
        let enum16 = remove_white_spaces("Enum16 ()");

        assert!(dbg!(parse_enum16(enum16.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum16_no_value() {
        let enum16 = remove_white_spaces("Enum16 ('a' =)");

        assert!(dbg!(parse_enum16(enum16.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum16_no_ident() {
        let enum16 = remove_white_spaces("Enum16 ( = 1)");

        assert!(dbg!(parse_enum16(enum16.as_str())).is_none());
    }

    #[test]
    fn test_parse_enum16_starting_comma() {
        let enum16 = remove_white_spaces("Enum16 ( , 'a' = 1)");

        assert!(dbg!(parse_enum16(enum16.as_str())).is_none());
    }
}
