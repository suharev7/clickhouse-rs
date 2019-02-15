use std::io::Write;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Error,
    types::{
        column::{nullable::NullableColumnData, ColumnWrapper, StringPool},
        SqlType, Value, ValueRef,
    },
};

use super::{column_data::ColumnData, ColumnFrom};

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
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
        W::wrap(StringColumnData { pool: data.into() })
    }
}

impl<'a> ColumnFrom for Vec<&'a str> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let data: Vec<_> = source.iter().map(|s| s.to_string()).collect();
        W::wrap(StringColumnData { pool: data.into() })
    }
}

impl ColumnFrom for Vec<Option<String>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let inner = Box::new(StringColumnData::with_capacity(source.len()));

        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };

        for value in source {
            data.push(value.into());
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Option<&str>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let inner = Box::new(StringColumnData::with_capacity(source.len()));

        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };

        for value in source {
            data.push(value.into());
        }

        W::wrap(data)
    }
}

impl ColumnData for StringColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::String
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let strings = self.pool.strings();
        for v in strings.skip(start).take(end - start) {
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
