use std::{io::Write, string::ToString};

use crate::{
    binary::{Encoder, ReadEx},
    errors::Error,
    types::{
        column::{
            array::ArrayColumnData, list::List, nullable::NullableColumnData, BoxColumnWrapper,
            ColumnWrapper, StringPool,
        },
        Column, FromSql, SqlType, Value, ValueRef,
    },
};

use super::{column_data::ColumnData, ColumnFrom};

pub struct StringColumnData {
    pool: StringPool,
}

pub(crate) struct StringAdapter {
    pub(crate) column: Column,
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
        let data: Vec<_> = source.iter().map(ToString::to_string).collect();
        W::wrap(StringColumnData { pool: data.into() })
    }
}

impl<'a> ColumnFrom for Vec<&'a [u8]> {
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
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

impl ColumnFrom for Vec<Vec<String>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let fake: Vec<String> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<BoxColumnWrapper>(fake);
        let sql_type = inner.sql_type();

        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };

        for vs in source {
            let mut inner = Vec::with_capacity(vs.len());
            for v in vs {
                let value: Value = v.into();
                inner.push(value)
            }
            data.push(Value::Array(sql_type, inner));
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Vec<&str>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let fake: Vec<&str> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<BoxColumnWrapper>(fake);
        let sql_type = inner.sql_type();

        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };

        for vs in source {
            let mut inner = Vec::with_capacity(vs.len());
            for v in vs {
                let value: Value = v.into();
                inner.push(value)
            }
            data.push(Value::Array(sql_type, inner));
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Option<Vec<u8>>> {
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
        let s: Vec<u8> = value.into();
        let mut b = self.pool.allocate(s.len());
        b.write_all(s.as_ref()).unwrap();
    }

    fn at(&self, index: usize) -> ValueRef {
        let s = self.pool.get(index);
        ValueRef::from(s)
    }
}

impl ColumnData for StringAdapter {
    fn sql_type(&self) -> SqlType {
        SqlType::String
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for index in start..end {
            let buf: Vec<u8> = Vec::from_sql(self.column.at(index)).unwrap();
            encoder.byte_string(buf);
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        self.column.at(index)
    }
}
