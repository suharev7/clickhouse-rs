use chrono_tz::Tz;

use crate::types::column::Either;
use crate::types::enums::{Enum16, Enum8};
use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            column_data::BoxColumnData, column_data::ColumnData, list::List,
            nullable::NullableColumnData, BoxColumnWrapper, ColumnFrom, ColumnWrapper,
            VectorColumnData,
        },
        from_sql::FromSql,
        Column, ColumnType, SqlType, Value, ValueRef,
    },
};

pub(crate) struct Enum16ColumnData {
    pub(crate) enum_values: Vec<(String, i16)>,
    pub(crate) inner: Box<dyn ColumnData + Send + Sync>,
}

pub(crate) struct Enum16Adapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) enum_values: Vec<(String, i16)>,
}

pub(crate) struct NullableEnum16Adapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) enum_values: Vec<(String, i16)>,
}

impl Enum16ColumnData {
    pub(crate) fn load<T: ReadEx>(
        reader: &mut T,
        enum_values: Vec<(String, i16)>,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let type_name = "Int16";

        let inner = ColumnData::load_data::<BoxColumnWrapper, _>(reader, type_name, size, tz)?;

        Ok(Enum16ColumnData { enum_values, inner })
    }
}

impl ColumnData for Enum16ColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Enum16(self.enum_values.clone())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        self.inner.save(encoder, start, end)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Enum16(_values, enum_value) = value {
            self.inner.push(Value::Int16(enum_value.internal()))
        } else {
            panic!("value should be Enum ({:?})", value);
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let enum_value = i16::from(self.inner.at(index));
        ValueRef::Enum16(self.enum_values.clone(), Enum16(enum_value))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone_instance(),
            enum_values: self.enum_values.clone(),
        })
    }
}

impl<K: ColumnType> ColumnData for Enum16Adapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Enum16(self.enum_values.clone())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for i in start..end {
            if let ValueRef::Enum16(_enum_values, value) = self.at(i) {
                encoder.write(value.internal())
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
        if let ValueRef::Enum16(enum_values, value) = self.column.at(index) {
            ValueRef::Enum16(enum_values, value)
        } else {
            panic!("should be Enum");
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl<K: ColumnType> ColumnData for NullableEnum16Adapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Nullable(SqlType::Enum16(self.enum_values.clone()).into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let size = end - start;
        let mut nulls = vec![0; size];
        let mut values: Vec<Option<i16>> = vec![None; size];

        for (i, index) in (start..end).enumerate() {
            let value: Option<Enum16> = Option::from_sql(self.at(index)).unwrap();
            match value {
                Some(v) => values[i] = Some(v.internal()),
                None => nulls[i] = 1,
            }
        }

        encoder.write_bytes(nulls.as_ref());

        for value in values {
            let data_value = value.unwrap_or(0);
            encoder.write(data_value);
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let value: Option<Enum16> = Option::from_sql(self.column.at(index)).unwrap();
        match value {
            None => ValueRef::Nullable(Either::Left(self.sql_type().into())),
            Some(v) => {
                let inner = ValueRef::Enum16(self.enum_values.clone(), v);
                ValueRef::Nullable(Either::Right(Box::new(inner)))
            }
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl ColumnFrom for Vec<Enum16> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<i16>::with_capacity(source.len());

        for s in source {
            data.push(s.internal());
        }
        let inner = Box::new(VectorColumnData { data });

        let column = Enum16ColumnData {
            inner,
            enum_values: vec![],
        };

        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Option<Enum16>> {
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

        let inner = Enum16ColumnData {
            enum_values: vec![],
            inner,
        };

        W::wrap(NullableColumnData {
            nulls,
            inner: Box::new(inner),
        })
    }
}

pub(crate) struct Enum8ColumnData {
    pub(crate) enum_values: Vec<(String, i8)>,
    pub(crate) inner: Box<dyn ColumnData + Send + Sync>,
}

pub(crate) struct Enum8Adapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) enum_values: Vec<(String, i8)>,
}

pub(crate) struct NullableEnum8Adapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) enum_values: Vec<(String, i8)>,
}

impl Enum8ColumnData {
    pub(crate) fn load<T: ReadEx>(
        reader: &mut T,
        enum_values: Vec<(String, i8)>,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let type_name = "Int8";

        let inner = ColumnData::load_data::<BoxColumnWrapper, _>(reader, type_name, size, tz)?;

        Ok(Enum8ColumnData { enum_values, inner })
    }
}

impl ColumnData for Enum8ColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Enum8(self.enum_values.clone())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        self.inner.save(encoder, start, end)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Enum8(_values, enum_value) = value {
            self.inner.push(Value::Int8(enum_value.internal()))
        } else {
            panic!("value should be Enum ({:?})", value);
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let enum_value = i8::from(self.inner.at(index));
        ValueRef::Enum8(self.enum_values.clone(), Enum8(enum_value))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone_instance(),
            enum_values: self.enum_values.clone(),
        })
    }
}

impl<K: ColumnType> ColumnData for Enum8Adapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Enum8(self.enum_values.clone())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for i in start..end {
            if let ValueRef::Enum8(_enum_values, value) = self.at(i) {
                encoder.write(value.internal())
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
        if let ValueRef::Enum8(enum_values, value) = self.column.at(index) {
            ValueRef::Enum8(enum_values, value)
        } else {
            panic!("should be Enum");
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl<K: ColumnType> ColumnData for NullableEnum8Adapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Nullable(SqlType::Enum8(self.enum_values.clone()).into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let size = end - start;
        let mut nulls = vec![0; size];
        let mut values: Vec<Option<i8>> = vec![None; size];

        for (i, index) in (start..end).enumerate() {
            let value: Option<Enum8> = Option::from_sql(self.at(index)).unwrap();
            match value {
                Some(v) => values[i] = Some(v.internal()),
                None => nulls[i] = 1,
            }
        }

        encoder.write_bytes(nulls.as_ref());

        for value in values {
            let data_value = value.unwrap_or(0);
            encoder.write(data_value);
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let value: Option<Enum8> = Option::from_sql(self.column.at(index)).unwrap();
        match value {
            None => ValueRef::Nullable(Either::Left(self.sql_type().into())),
            Some(v) => {
                let inner = ValueRef::Enum8(self.enum_values.clone(), v);
                ValueRef::Nullable(Either::Right(Box::new(inner)))
            }
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl ColumnFrom for Vec<Enum8> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<i8>::with_capacity(source.len());

        for s in source {
            data.push(s.internal());
        }
        let inner = Box::new(VectorColumnData { data });

        let column = Enum8ColumnData {
            inner,
            enum_values: vec![],
        };

        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Option<Enum8>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut nulls: Vec<u8> = Vec::with_capacity(source.len());
        let mut data = List::<i8>::with_capacity(source.len());

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

        let inner = Enum8ColumnData {
            enum_values: vec![],
            inner,
        };

        W::wrap(NullableColumnData {
            nulls,
            inner: Box::new(inner),
        })
    }
}
