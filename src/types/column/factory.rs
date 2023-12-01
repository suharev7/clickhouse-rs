use chrono_tz::Tz;

use combine::{
    any, choice,
    error::StringStreamError,
    many, many1, none_of, optional,
    parser::char::{digit, spaces, string},
    sep_by1, token, Parser,
};

use crate::{
    binary::ReadEx,
    errors::Result,
    types::{
        column::{
            array::ArrayColumnData,
            chrono_datetime::ChronoDateTimeColumnData,
            column_data::ColumnData,
            date::DateColumnData,
            datetime64::DateTime64ColumnData,
            decimal::DecimalColumnData,
            enums::{Enum16ColumnData, Enum8ColumnData},
            fixed_string::FixedStringColumnData,
            ip::{IpColumnData, Ipv4, Ipv6, Uuid},
            list::List,
            low_cardinality::LowCardinalityColumnData,
            map::MapColumnData,
            nullable::NullableColumnData,
            numeric::VectorColumnData,
            simple_agg_func::SimpleAggregateFunctionColumnData,
            string::StringColumnData,
            ArcColumnWrapper, BoxColumnWrapper, ColumnWrapper,
        },
        decimal::NoBits,
        DateTimeType, SimpleAggFunc,
    },
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
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn load_data<W: ColumnWrapper, T: ReadEx>(
        reader: &mut T,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<W::Wrapper> {
        Ok(match_str!(type_name, {
            "Bool" => W::wrap(VectorColumnData::<bool>::load(reader, size)?),
            "UInt8" => W::wrap(VectorColumnData::<u8>::load(reader, size)?),
            "UInt16" => W::wrap(VectorColumnData::<u16>::load(reader, size)?),
            "UInt32" => W::wrap(VectorColumnData::<u32>::load(reader, size)?),
            "UInt64" => W::wrap(VectorColumnData::<u64>::load(reader, size)?),
            "UInt128" => W::wrap(VectorColumnData::<u128>::load(reader, size)?),
            "Int8" | "TinyInt" => W::wrap(VectorColumnData::<i8>::load(reader, size)?),
            "Int16" | "SmallInt" => W::wrap(VectorColumnData::<i16>::load(reader, size)?),
            "Int32" | "Int" | "Integer" => W::wrap(VectorColumnData::<i32>::load(reader, size)?),
            "Int64" | "BigInt" => W::wrap(VectorColumnData::<i64>::load(reader, size)?),
            "Int128" => W::wrap(VectorColumnData::<i128>::load(reader, size)?),
            "Float32" | "Float" => W::wrap(VectorColumnData::<f32>::load(reader, size)?),
            "Float64" | "Double" => W::wrap(VectorColumnData::<f64>::load(reader, size)?),
            "String" | "Char" | "Varchar" | "Text" | "TinyText" | "MediumText" | "LongText" | "Blob" | "TinyBlob" | "MediumBlob" | "LongBlob" => W::wrap(StringColumnData::load(reader, size)?),
            "Date" => W::wrap(DateColumnData::<u16>::load(reader, size, tz)?),
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
                } else if let Some(inner_type) = parse_map_type(type_name) {
                    W::wrap(MapColumnData::load(reader, inner_type, size, tz)?)
                } else if let Some((precision, scale, nobits)) = parse_decimal(type_name) {
                    W::wrap(DecimalColumnData::load(
                        reader, precision, scale, nobits, size, tz,
                    )?)
                } else if let Some(items) = parse_enum8(type_name) {
                    W::wrap(Enum8ColumnData::load(reader, items, size, tz)?)
                } else if let Some(items) = parse_enum16(type_name) {
                    W::wrap(Enum16ColumnData::load(reader, items, size, tz)?)
                } else if let Some(timezone) = parse_date_time(type_name) {
                    let column_timezone = get_timezone(&timezone, tz)?;
                    W::wrap(DateColumnData::<u32>::load(reader, size, column_timezone)?)
                } else if let Some((precision, timezone)) = parse_date_time64(type_name) {
                    let column_timezone = get_timezone(&timezone, tz)?;
                    W::wrap(DateTime64ColumnData::load(reader, size, precision, column_timezone)?)
                } else if let Some((func, inner_type)) = parse_simple_agg_fun(type_name) {
                    W::wrap(SimpleAggregateFunctionColumnData::load(reader, func, inner_type, size, tz)?)
                } else if let Some(inner_type) = parse_low_cardinality(type_name) {
                    W::wrap(LowCardinalityColumnData::load(reader, inner_type, size, tz)?)
                } else {
                    let message = format!("Unsupported column type \"{type_name}\".");
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
            SqlType::Bool => W::wrap(VectorColumnData::<bool>::with_capacity(capacity)),
            SqlType::UInt8 => W::wrap(VectorColumnData::<u8>::with_capacity(capacity)),
            SqlType::UInt16 => W::wrap(VectorColumnData::<u16>::with_capacity(capacity)),
            SqlType::UInt32 => W::wrap(VectorColumnData::<u32>::with_capacity(capacity)),
            SqlType::UInt64 => W::wrap(VectorColumnData::<u64>::with_capacity(capacity)),
            SqlType::UInt128 => W::wrap(VectorColumnData::<u128>::with_capacity(capacity)),
            SqlType::Int8 => W::wrap(VectorColumnData::<i8>::with_capacity(capacity)),
            SqlType::Int16 => W::wrap(VectorColumnData::<i16>::with_capacity(capacity)),
            SqlType::Int32 => W::wrap(VectorColumnData::<i32>::with_capacity(capacity)),
            SqlType::Int64 => W::wrap(VectorColumnData::<i64>::with_capacity(capacity)),
            SqlType::Int128 => W::wrap(VectorColumnData::<i128>::with_capacity(capacity)),
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
            SqlType::DateTime(DateTimeType::DateTime64(precision, timezone)) => W::wrap(
                DateTime64ColumnData::with_capacity(capacity, precision, timezone),
            ),
            SqlType::DateTime(_) => {
                W::wrap(ChronoDateTimeColumnData::with_capacity(capacity, timezone))
            }
            SqlType::Nullable(inner_type) => W::wrap(NullableColumnData {
                inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    inner_type.clone(),
                    timezone,
                    capacity,
                )?,
                nulls: Vec::new(),
            }),
            SqlType::Array(inner_type) => W::wrap(ArrayColumnData {
                inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    inner_type.clone(),
                    timezone,
                    capacity,
                )?,
                offsets: List::with_capacity(capacity),
            }),
            SqlType::SimpleAggregateFunction(func, inner_type) => {
                W::wrap(SimpleAggregateFunctionColumnData {
                    inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                        inner_type.clone(),
                        timezone,
                        capacity,
                    )?,
                    func,
                })
            }
            SqlType::Decimal(precision, scale) => {
                let nobits = NoBits::from_precision(precision).unwrap();

                let inner_type = match nobits {
                    NoBits::N32 => SqlType::Int32,
                    NoBits::N64 => SqlType::Int64,
                    NoBits::N128 => SqlType::Int128,
                };

                W::wrap(DecimalColumnData {
                    inner: <dyn ColumnData>::from_type::<BoxColumnWrapper>(
                        inner_type, timezone, capacity,
                    )?,
                    precision,
                    scale,
                    nobits,
                })
            }
            SqlType::Enum8(enum_values) => W::wrap(Enum8ColumnData {
                enum_values,
                inner: <dyn ColumnData>::from_type::<BoxColumnWrapper>(
                    SqlType::Int8,
                    timezone,
                    capacity,
                )?,
            }),
            SqlType::Enum16(enum_values) => W::wrap(Enum16ColumnData {
                enum_values,
                inner: <dyn ColumnData>::from_type::<BoxColumnWrapper>(
                    SqlType::Int16,
                    timezone,
                    capacity,
                )?,
            }),
            SqlType::Map(k, v) => W::wrap(MapColumnData {
                keys: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    k.clone(),
                    timezone,
                    capacity,
                )?,
                offsets: List::with_capacity(capacity),
                values: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    v.clone(),
                    timezone,
                    capacity,
                )?,
                size: 0,
            }),
            SqlType::LowCardinality(inner) => {
                W::wrap(
                    LowCardinalityColumnData::empty(inner, timezone, capacity)?, // LowCardinalityColumnData {
                                                                                 //     inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                                                                                 //         inner.clone(),
                                                                                 //         timezone,
                                                                                 //         capacity
                                                                                 //     )?,
                                                                                 //     index: Index::UInt8(
                                                                                 //         VectorColumnData::<u8>::with_capacity(capacity)
                                                                                 //     ),
                                                                                 //     value_map: None,
                                                                                 // }
                )
            }
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

fn parse_map_type(source: &str) -> Option<(&str, &str)> {
    if !source.starts_with("Map") {
        return None;
    }

    let body = &source[4..source.len() - 1];
    let comma_pos = body.chars().position(|c| c == ',')?;

    let key = body[0..comma_pos].trim();
    let value = body[comma_pos + 1..].trim();

    Some((key, value))
}

fn parse_simple_agg_fun(source: &str) -> Option<(SimpleAggFunc, &str)> {
    if !source.starts_with("SimpleAggregateFunction(") || !source.ends_with(')') {
        return None;
    }

    let args = source[23..].trim_matches(|c| c == '(' || c == ')');
    let sep_index = args.find(',')?;

    let agg_func = args[..sep_index].trim();
    let agg_type = args[sep_index + 1..].trim();

    let func: SimpleAggFunc = match agg_func.parse() {
        Ok(func) => func,
        Err(_) => return None,
    };

    Some((func, agg_type))
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
            scale = (source[params_indexes.0 + 1..params_indexes.1])
                .parse()
                .ok()
        }
        None => {
            for (idx, cell) in (source[params_indexes.0 + 1..params_indexes.1])
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

            NoBits::from_precision(precision).map(|nobits| (precision, scale, nobits))
        }
        (None, Some(scale), Some(bits)) => {
            let precision = match bits {
                NoBits::N32 => 9,
                NoBits::N64 => 18,
                NoBits::N128 => 38,
            };
            Some((precision, scale, bits))
        }
        _ => None,
    }
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
                .map(|(key, val)| (key.clone(), *val as i8))
                .collect();
            Some(res)
        }
        None => None,
    }
}
fn parse_enum16(input: &str) -> Option<Vec<(String, i16)>> {
    parse_enum(EnumSize::Enum16, input)
}

fn parse_enum(size: EnumSize, input: &str) -> Option<Vec<(String, i16)>> {
    let size = match size {
        EnumSize::Enum8 => "Enum8",
        EnumSize::Enum16 => "Enum16",
    };

    let integer = optional(token('-'))
        .and(many1::<String, _, _>(digit()))
        .and_then(|(x, mut digits)| {
            if let Some(x) = x {
                digits.insert(0, x);
            }
            digits
                .parse::<i16>()
                .map_err(|_| StringStreamError::UnexpectedParse)
        });

    let word_syms = token('\\').with(any()).or(none_of("'".chars()));
    let word = token('\'').with(many(word_syms)).skip(token('\''));

    let pair = spaces()
        .with(word)
        .skip(spaces())
        .skip(token('='))
        .skip(spaces())
        .and(integer)
        .skip(spaces());
    let enum_body = sep_by1::<Vec<(String, i16)>, _, _, _>(pair, token(','));

    let mut parser = spaces()
        .with(string(size))
        .skip(spaces())
        .skip(token('('))
        .skip(spaces())
        .with(enum_body)
        .skip(token(')'));
    let result = parser.parse(input);
    if let Ok((res, remain)) = result {
        if !remain.is_empty() {
            return None;
        }
        Some(res)
    } else {
        None
    }
}

fn parse_date_time(source: &str) -> Option<Option<String>> {
    let word_syms = token('\\').with(any()).or(none_of("'".chars()));
    let word = token('\'')
        .with(many::<String, _, _>(word_syms))
        .skip(token('\''));

    let mut parser = spaces()
        .with(choice((string("DateTime"), string("Timestamp"))))
        .with(optional(
            spaces()
                .skip(token('('))
                .skip(spaces())
                .with(word)
                .skip(spaces())
                .skip(token(')'))
                .skip(spaces()),
        ));

    match parser.parse(source) {
        Ok((timezone, remain)) if remain.is_empty() => Some(timezone),
        _ => None,
    }
}

fn parse_date_time64(source: &str) -> Option<(u32, Option<String>)> {
    let integer = many1::<String, _, _>(digit()).and_then(|digits| {
        digits
            .parse::<u32>()
            .map_err(|_| StringStreamError::UnexpectedParse)
    });

    let word_syms = token('\\').with(any()).or(none_of("'".chars()));
    let word = token('\'')
        .with(many::<String, _, _>(word_syms))
        .skip(token('\''));

    let timezone = optional(spaces().skip(token(',')).skip(spaces()).with(word));

    let pair = spaces()
        .with(integer)
        .skip(spaces())
        .and(timezone)
        .skip(spaces());

    let mut parser = spaces()
        .with(string("DateTime64"))
        .skip(spaces())
        .skip(token('('))
        .skip(spaces())
        .with(pair)
        .skip(spaces())
        .skip(token(')'));

    match parser.parse(source) {
        Ok((pair, remain)) if remain.is_empty() => Some(pair),
        _ => None,
    }
}

fn parse_low_cardinality(source: &str) -> Option<&str> {
    if !source.starts_with("LowCardinality") {
        return None;
    }

    let mut lo = 14;
    let mut hi = source.len() - 1;

    while lo < source.len() && &source[lo..lo + 1] != "(" {
        lo += 1;
    }

    while hi > lo && &source[hi..hi + 1] != ")" {
        hi -= 1;
    }

    if lo >= hi {
        return None;
    }

    Some(source[lo + 1..hi].trim())
}

fn get_timezone(timezone: &Option<String>, tz: Tz) -> Result<Tz> {
    match timezone {
        None => Ok(tz),
        Some(t) => Ok(t.parse()?),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_decimal() {
        assert_eq!(parse_decimal("Decimal(9, 4)"), Some((9, 4, NoBits::N32)));
        assert_eq!(parse_decimal("Decimal(10, 4)"), Some((10, 4, NoBits::N64)));
        assert_eq!(parse_decimal("Decimal(20, 4)"), Some((20, 4, NoBits::N128)));
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

    #[test]
    fn test_parse_enum8() {
        let enum8 = "Enum8 ('a' = 1, 'b' = 2)";

        let res = parse_enum8(enum8).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1), ("b".to_owned(), 2)])
    }
    #[test]
    fn test_parse_enum16_special_chars() {
        let enum16 = "Enum16('a_' = -128, 'b&' = 0)";

        let res = parse_enum16(enum16).unwrap();
        assert_eq!(res, vec![("a_".to_owned(), -128), ("b&".to_owned(), 0)])
    }

    #[test]
    fn test_parse_enum8_single() {
        let enum8 = "Enum8 ('a' = 1)";

        let res = parse_enum8(enum8).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum8_empty_id() {
        let enum8 = "Enum8 ('' = 1, '' = 2)";

        let res = parse_enum8(enum8).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1), ("".to_owned(), 2)])
    }

    #[test]
    fn test_parse_enum8_single_empty_id() {
        let enum8 = "Enum8 ('' = 1)";

        let res = parse_enum8(enum8).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum8_extra_comma() {
        let enum8 = "Enum8 ('a' = 1, 'b' = 2,)";

        assert!(dbg!(parse_enum8(enum8)).is_none());
    }

    #[test]
    fn test_parse_enum8_empty() {
        let enum8 = "Enum8 ()";

        assert!(dbg!(parse_enum8(enum8)).is_none());
    }

    #[test]
    fn test_parse_enum8_no_value() {
        let enum8 = "Enum8 ('a' =)";

        assert!(dbg!(parse_enum8(enum8)).is_none());
    }

    #[test]
    fn test_parse_enum8_no_ident() {
        let enum8 = "Enum8 ( = 1)";

        assert!(dbg!(parse_enum8(enum8)).is_none());
    }

    #[test]
    fn test_parse_enum8_starting_comma() {
        let enum8 = "Enum8 ( , 'a' = 1)";

        assert!(dbg!(parse_enum8(enum8)).is_none());
    }

    #[test]
    fn test_parse_enum16() {
        let enum16 = "Enum16 ('a' = 1, 'b' = 2)";

        let res = parse_enum16(enum16).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1), ("b".to_owned(), 2)])
    }

    #[test]
    fn test_parse_enum16_single() {
        let enum16 = "Enum16 ('a' = 1)";

        let res = parse_enum16(enum16).unwrap();
        assert_eq!(res, vec![("a".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum16_empty_id() {
        let enum16 = "Enum16 ('' = 1, '' = 2)";

        let res = parse_enum16(enum16).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1), ("".to_owned(), 2)])
    }

    #[test]
    fn test_parse_enum16_single_empty_id() {
        let enum16 = "Enum16 ('' = 1)";

        let res = parse_enum16(enum16).unwrap();
        assert_eq!(res, vec![("".to_owned(), 1)])
    }

    #[test]
    fn test_parse_enum16_extra_comma() {
        let enum16 = "Enum16 ('a' = 1, 'b' = 2,)";

        assert!(dbg!(parse_enum16(enum16)).is_none());
    }

    #[test]
    fn test_parse_enum16_empty() {
        let enum16 = "Enum16 ()";

        assert!(dbg!(parse_enum16(enum16)).is_none());
    }

    #[test]
    fn test_parse_enum16_no_value() {
        let enum16 = "Enum16 ('a' =)";

        assert!(dbg!(parse_enum16(enum16)).is_none());
    }

    #[test]
    fn test_parse_enum16_no_ident() {
        let enum16 = "Enum16 ( = 1)";

        assert!(dbg!(parse_enum16(enum16)).is_none());
    }

    #[test]
    fn test_parse_enum16_starting_comma() {
        let enum16 = "Enum16 ( , 'a' = 1)";

        assert!(dbg!(parse_enum16(enum16)).is_none());
    }

    #[test]
    fn test_parse_date_time() {
        let source = " DateTime ( 'Europe/Moscow' )";
        let res = parse_date_time(source).unwrap();
        assert_eq!(res, Some("Europe/Moscow".to_string()))
    }

    #[test]
    fn test_parse_date_time_without_timezone() {
        let source = " DateTime";
        let res = parse_date_time(source).unwrap();
        assert_eq!(res, None)
    }

    #[test]
    fn test_parse_date_time64() {
        let source = " DateTime64 ( 3 , 'Europe/Moscow' )";
        let res = parse_date_time64(source).unwrap();
        assert_eq!(res, (3, Some("Europe/Moscow".to_string())))
    }

    #[test]
    fn test_parse_date_time64_without_timezone() {
        let source = " DateTime64( 5 )";
        let res = parse_date_time64(source).unwrap();
        assert_eq!(res, (5, None))
    }

    #[test]
    fn test_parse_simple_agg_fun() {
        let expected = Some((SimpleAggFunc::Sum, "Double"));
        let actual = parse_simple_agg_fun("SimpleAggregateFunction( sum , Double )");
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_parse_map_type() {
        let s = "Map(UInt8, Map(UInt8,UInt8))";
        let actual = parse_map_type(s);
        let expected = Some(("UInt8", "Map(UInt8,UInt8)"));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_low_cardinality() {
        assert_eq!(
            parse_low_cardinality("LowCardinality ( String )"),
            Some("String")
        );
    }
}
