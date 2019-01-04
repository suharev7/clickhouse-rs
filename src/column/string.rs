use std::sync::Arc;

use crate::binary::{Encoder, ReadEx};
use crate::column::{column_data::ColumnData, BoxColumnData, ColumnFrom};
use crate::errors::Error;
use crate::types::{SqlType, Value, ValueRef};

pub struct StringColumnData {
    data: Vec<String>,
}

impl StringColumnData {
    pub fn with_capacity(capacity: usize) -> StringColumnData {
        StringColumnData {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn load<T: ReadEx>(reader: &mut T, size: usize) -> Result<StringColumnData, Error> {
        let mut data = StringColumnData::with_capacity(size);

        for _ in 0..size {
            data.push(Value::from(reader.read_string()?));
        }

        Ok(data)
    }
}

impl ColumnFrom for Vec<String> {
    fn column_from(data: Vec<String>) -> BoxColumnData {
        Arc::new(StringColumnData { data })
    }
}

impl<'a> ColumnFrom for Vec<&'a str> {
    fn column_from(source: Vec<&'a str>) -> BoxColumnData {
        let data = source.iter().map(|s| s.to_string()).collect();
        Arc::new(StringColumnData { data })
    }
}

impl ColumnData for StringColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::String
    }

    fn save(&self, encoder: &mut Encoder) {
        for v in &self.data {
            encoder.string(v);
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        self.data.push(value.into())
    }

    fn at(&self, index: usize) -> ValueRef {
        let s: &str = &self.data[index];
        ValueRef::from(s)
    }
}
