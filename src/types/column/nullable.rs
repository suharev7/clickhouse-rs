use std::sync::Arc;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            column_data::{ArcColumnData, BoxColumnData},
            ArcColumnWrapper, ColumnData,
        },
        SqlType, Value, ValueRef,
    },
};

use chrono_tz::Tz;
use either::Either;

pub(crate) struct NullableColumnData {
    pub(crate) inner: ArcColumnData,
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
        let inner =
            <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, type_name, size, tz)?;
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
        let inner_column: &mut dyn ColumnData = Arc::get_mut(&mut self.inner).unwrap();

        if let Value::Nullable(e) = value {
            match e {
                Either::Left(sql_type) => {
                    let default_value = Value::default(sql_type.clone());
                    inner_column.push(default_value);
                    self.nulls.push(true as u8);
                }
                Either::Right(inner) => {
                    inner_column.push(*inner);
                    self.nulls.push(false as u8);
                }
            }
        } else {
            inner_column.push(value);
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
            inner: self.inner.clone(),
            nulls: self.nulls.clone(),
        })
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        props: u32,
    ) -> Result<()> {
        if level == self.sql_type().level() {
            *pointers[0] = self.nulls.as_ptr();
            *(pointers[1] as *mut usize) = self.len();
            Ok(())
        } else {
            self.inner.get_internal(pointers, level, props)
        }
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if let SqlType::Nullable(inner_target) = target {
            if let Some(inner) = self.inner.cast_to(&self.inner, inner_target) {
                return Some(Arc::new(NullableColumnData {
                    inner,
                    nulls: self.nulls.clone(),
                }));
            }
        }
        None
    }
}
