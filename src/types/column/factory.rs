use chrono_tz::Tz;

use crate::{binary::ReadEx, errors::Error};

use super::{
    column_data::ColumnData, date::DateColumnData, nullable::NullableColumnData,
    numeric::VectorColumnData, string::StringColumnData, ColumnWrapper,
};
use crate::types::column::fixed_string::FixedStringColumnData;

impl ColumnData {
    pub(crate) fn load_data<W: ColumnWrapper, T: ReadEx>(
        reader: &mut T,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<W::Wrapper, Error> {
        Ok(match type_name {
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
            _ => {
                if let Some(inner_type) = parse_nullable_type(type_name) {
                    W::wrap(NullableColumnData::load(reader, inner_type, size, tz)?)
                } else if let Some(str_len) = parse_fixed_string(type_name) {
                    W::wrap(FixedStringColumnData::load(reader, size, str_len)?)
                } else {
                    let message = format!("Unsupported column type \"{}\".", type_name);
                    return Err(message.into());
                }
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

#[cfg(test)]
mod test {
    use super::*;

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
