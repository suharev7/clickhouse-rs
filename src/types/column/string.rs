use std::{io::Write, sync::Arc};

use crate::{
    binary::{Encoder, ReadEx},
    errors::Error,
    types::{SqlType, Value, ValueRef},
};

use super::{BoxColumnData, column_data::ColumnData, ColumnFrom};
use crate::types::column::string_pool::StringPool;

pub struct StringColumnData {
    pool: StringPool,
}

impl StringColumnData {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pool: StringPool::with_capacity(capacity),
        }
    }

    pub(crate) fn load<T: ReadEx>(reader: &mut T, size: usize) -> Result<Self, Error> {

        let mut data = Self::with_capacity(size);

        for _ in 0..size {
            reader.read_str_into_buffer(&mut data.pool)?;
        }

        Ok(data)
    }
}

impl ColumnFrom for Vec<String> {
    fn column_from(data: Self) -> BoxColumnData {
        Arc::new(StringColumnData { pool: data.into() })
    }
}

impl<'a> ColumnFrom for Vec<&'a str> {
    fn column_from(source: Self) -> BoxColumnData {
        let data: Vec<_> = source.iter().map(|s| s.to_string()).collect();
        Arc::new(StringColumnData { pool: data.into() })
    }
}

impl ColumnData for StringColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::String
    }

    fn save(&self, encoder: &mut Encoder) {
        for v in self.pool.strings() {
            encoder.string(v);
        }
    }

    fn len(&self) -> usize {
        self.pool.len()
    }

    fn push(&mut self, value: Value) {
        let s: String = value.into();
        let mut b = self.pool.allocate(s.len());
        b.write_all(s.as_bytes()).unwrap();
    }

    fn at(&self, index: usize) -> ValueRef {
        let s = self.pool.get(index);
        ValueRef::from(s)
    }
}
