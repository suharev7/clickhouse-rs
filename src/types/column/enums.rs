use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            BoxColumnWrapper, column_data::BoxColumnData, column_data::ColumnData,
            ColumnFrom, ColumnWrapper, list::List, nullable::NullableColumnData,
            VectorColumnData,
        },
        Column,
        ColumnType,
        from_sql::FromSql, SqlType, Value, ValueRef,
    },
};
use crate::types::column::Either;
use crate::types::enums::{Enum, EnumSize};

pub(crate) struct EnumColumnData {
    pub(crate) enum_values: Vec<(String, i16)>,
    pub(crate) size: EnumSize,
    pub(crate) inner: Box<dyn ColumnData + Send + Sync>,
}

pub(crate) struct EnumAdapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) size: EnumSize,
    pub(crate) enum_values: Vec<(String, i16)>,
}

pub(crate) struct NullableEnumAdapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) enum_values: Vec<(String, i16)>,
    pub(crate) size: EnumSize,
}

impl EnumColumnData {
    pub(crate) fn load<T: ReadEx>(
        reader: &mut T,
        enum_size: EnumSize,
        enum_values: Vec<(String, i16)>,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let type_name = match enum_size {
            EnumSize::ENUM8 => "Int8",
            EnumSize::ENUM16 => "Int16"
        };

        let inner = ColumnData::load_data::<BoxColumnWrapper, _>(reader, type_name, size, tz)?;

        Ok(EnumColumnData {
            size: enum_size,
            enum_values,
            inner,
        })
    }
}

impl ColumnData for EnumColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Enum(self.size, self.enum_values.clone())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        self.inner.save(encoder, start, end)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Enum(size, _values, enum_value) = value {
            // TODO Validate enum or get string or smth else
            match size {
                EnumSize::ENUM8 => self.inner.push(Value::Int8(enum_value.internal() as i8)),
                EnumSize::ENUM16 => self.inner.push(Value::Int16(enum_value.internal()))
            }
        } else {
            panic!("value should be Enum ({:?})", value);
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        match self.size {
            EnumSize::ENUM8 => {
                let enum_value = i8::from(self.inner.at(index));
                ValueRef::Enum(self.size, self.enum_values.clone(), Enum(enum_value as i16))
            }
            EnumSize::ENUM16 => {
                let enum_value = i16::from(self.inner.at(index));
                ValueRef::Enum(self.size, self.enum_values.clone(), Enum(enum_value))
            }
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone_instance(),
            enum_values: self.enum_values.clone(),
            size: self.size,
        })
    }
}

impl<K: ColumnType> ColumnData for EnumAdapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Enum(self.size, self.enum_values.clone())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for i in start..end {
            if let ValueRef::Enum(size, _enum_values, value) = self.at(i) {
                match size {
                    EnumSize::ENUM8 => encoder.write(value.internal() as i8),
                    EnumSize::ENUM16 => encoder.write(value.internal())
                }
            } else {
                panic!("should be Enum");
            }
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        if let ValueRef::Enum(size, enum_values, value) = self.column.at(index) {
            ValueRef::Enum(size, enum_values, value)
        } else {
            panic!("should be Enum");
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl<K: ColumnType> ColumnData for NullableEnumAdapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Nullable(SqlType::Enum(self.size, self.enum_values.clone()).into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let size = end - start;
        let mut nulls = vec![0; size];
        let mut values: Vec<Option<i8>> = vec![None; size];
        for (i, index) in (start..end).enumerate() {
            values[i] = Option::from_sql(self.at(index)).unwrap();
            if values[i].is_none() {
                nulls[i] = 1;
            }
        }

        encoder.write_bytes(nulls.as_ref());

        for value in values {
            let underlying = if let Some(v) = value { v } else { 0 };
            encoder.write(underlying);
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let value: Option<i16> = Option::from_sql(self.column.at(index)).unwrap();
        match value {
            None => ValueRef::Nullable(Either::Left(self.sql_type().into())),
            Some(v) => {
                let inner = ValueRef::Enum(self.size, self.enum_values.clone(), Enum(v));
                ValueRef::Nullable(Either::Right(Box::new(inner)))
            }
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl ColumnFrom for Vec<Enum> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<i16>::with_capacity(source.len());

        for s in source {
            data.push(s.internal());
        }
        let inner = Box::new(VectorColumnData { data });

        let column = EnumColumnData {
            inner,
            enum_values: vec![],
            size: EnumSize::ENUM16,
        };

        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Option<Enum>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut nulls: Vec<u8> = Vec::with_capacity(source.len());
        let mut data = List::<i16>::with_capacity(source.len());

        for os in source {
            if let Some(s) = os {
                data.push(s.internal());
                nulls.push(0);
            } else {
                data.push(0);
                nulls.push(1);
            }
        }
        let inner = Box::new(VectorColumnData { data });

        let inner = EnumColumnData {
            enum_values: vec![],
            size: EnumSize::ENUM16,
            inner,
        };

        W::wrap(NullableColumnData {
            nulls,
            inner: Box::new(inner),
        })
    }
}
