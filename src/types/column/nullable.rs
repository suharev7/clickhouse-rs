use crate::types::column::ColumnData;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{column_data::BoxColumnData, Either},
        SqlType, Value, ValueRef,
    },
};

use crate::types::column::BoxColumnWrapper;
use chrono_tz::Tz;

pub(crate) struct NullableColumnData {
    pub(crate) inner: Box<dyn ColumnData + Send + Sync>,
    pub(crate) nulls: Vec<u8>,
}

impl NullableColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let mut nulls = vec![0; size];
        reader.read_bytes(nulls.as_mut())?;

        let inner = ColumnData::load_data::<BoxColumnWrapper, _>(reader, type_name, size, tz)?;

        Ok(NullableColumnData { inner, nulls })
    }
}

impl ColumnData for NullableColumnData {
    fn sql_type(&self) -> SqlType {
        let inner_type = self.inner.sql_type();
        SqlType::Nullable(inner_type.into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let nulls: &[u8] = self.nulls.as_ref();
        encoder.write_bytes(&nulls[start..end]);
        self.inner.save(encoder, start, end);
    }

    fn len(&self) -> usize {
        assert_eq!(self.nulls.len(), self.inner.len());
        self.inner.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Nullable(e) = value {
            match e {
                Either::Left(sql_type) => {
                    let default_value = Value::default(sql_type.clone());
                    self.inner.push(default_value);
                    self.nulls.push(true as u8);
                }
                Either::Right(inner) => {
                    self.inner.push(*inner);
                    self.nulls.push(false as u8);
                }
            }
        } else {
            self.inner.push(value);
            self.nulls.push(false as u8);
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        if self.nulls[index] == 1 {
            let sql_type = self.inner.sql_type();
            ValueRef::Nullable(Either::Left(sql_type.into()))
        } else {
            let inner_value = self.inner.at(index);
            ValueRef::Nullable(Either::Right(Box::new(inner_value)))
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone_instance(),
            nulls: self.nulls.clone(),
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        if level == self.sql_type().level() {
            *pointers[0] = self.nulls.as_ptr();
            *(pointers[1] as *mut usize) = self.len();
            Ok(())
        } else {
            self.inner.get_internal(pointers, level)
        }
    }
}
