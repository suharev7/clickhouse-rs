use std::sync::Arc;

use chrono_tz::Tz;

use crate::{
    binary::ReadEx,
    errors::Error,
};

use super::{
    column_data::ColumnData,
    date::DateColumnData,
    numeric::VectorColumnData,
    string::StringColumnData,
};

impl ColumnData {
    pub fn load_data<T: ReadEx>(
        reader: &mut T,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<Arc<ColumnData + Send + Sync>, Error> {
        Ok(match type_name {
            "UInt8" => Arc::new(VectorColumnData::<u8>::load(reader, size)?),
            "UInt16" => Arc::new(VectorColumnData::<u16>::load(reader, size)?),
            "UInt32" => Arc::new(VectorColumnData::<u32>::load(reader, size)?),
            "UInt64" => Arc::new(VectorColumnData::<u64>::load(reader, size)?),
            "Int8" => Arc::new(VectorColumnData::<i8>::load(reader, size)?),
            "Int16" => Arc::new(VectorColumnData::<i16>::load(reader, size)?),
            "Int32" => Arc::new(VectorColumnData::<i32>::load(reader, size)?),
            "Int64" => Arc::new(VectorColumnData::<i64>::load(reader, size)?),
            "Float32" => Arc::new(VectorColumnData::<f32>::load(reader, size)?),
            "Float64" => Arc::new(VectorColumnData::<f64>::load(reader, size)?),
            "String" => Arc::new(StringColumnData::load(reader, size)?),
            "Date" => Arc::new(DateColumnData::<u16>::load(reader, size, tz)?),
            "DateTime" => Arc::new(DateColumnData::<u32>::load(reader, size, tz)?),
            _ => {
                let message = format!("Unsupported column type \"{}\".", type_name);
                return Err(message.into());
            }
        })
    }
}
