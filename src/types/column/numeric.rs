use std::{convert, mem, sync::Arc};

use crate::{
    binary::{Encoder, ReadEx},
    errors::Error,
    types::{Marshal, SqlType, StatBuffer, Unmarshal, Value, ValueRef},
};

use super::{BoxColumnData, column_data::ColumnData, ColumnFrom, list::List};

pub struct VectorColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Sync
        + Default
        + 'static,
{
    data: List<T>,
}

impl<T> ColumnFrom for Vec<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Send
        + Sync
        + Default
        + 'static,
{
    fn column_from(source: Self) -> BoxColumnData {
        let mut data = List::with_capacity(source.len());
        for s in source {
            data.push(s);
        }
        Arc::new(VectorColumnData { data })
    }
}

impl<T> VectorColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Sync
        + Default
        + 'static,
{
    #[cfg(test)]
    pub fn with_capacity(capacity: usize) -> VectorColumnData<T> {
        VectorColumnData {
            data: List::with_capacity(capacity),
        }
    }

    pub fn load<R: ReadEx>(reader: &mut R, size: usize) -> Result<VectorColumnData<T>, Error> {
        let mut row = vec![0_u8; size * mem::size_of::<T>()];
        reader.read_bytes(row.as_mut())?;
        let data = List::from(row);
        Ok(Self { data })
    }
}

impl<T> ColumnData for VectorColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Send
        + Sync
        + Default
        + 'static,
{
    fn sql_type(&self) -> SqlType {
        T::sql_type()
    }

    fn save(&self, encoder: &mut Encoder) {
        encoder.write_bytes(self.data.as_ref())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        self.data.push(T::from(value));
    }

    fn at(&self, index: usize) -> ValueRef {
        let v: Value = self.data.at(index).into();
        match v {
            Value::UInt8(x) => ValueRef::UInt8(x),
            Value::UInt16(x) => ValueRef::UInt16(x),
            Value::UInt32(x) => ValueRef::UInt32(x),
            Value::UInt64(x) => ValueRef::UInt64(x),

            Value::Int8(x) => ValueRef::Int8(x),
            Value::Int16(x) => ValueRef::Int16(x),
            Value::Int32(x) => ValueRef::Int32(x),
            Value::Int64(x) => ValueRef::Int64(x),

            Value::Float32(x) => ValueRef::Float32(x),
            Value::Float64(x) => ValueRef::Float64(x),

            _ => panic!("can't convert value to value_ref."),
        }
    }
}
