use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            column_data::{ArcColumnData, BoxColumnData},
            ArcColumnWrapper, ColumnData,
        },
        SimpleAggFunc, SqlType, Value, ValueRef,
    },
};

use chrono_tz::Tz;
use std::sync::Arc;

pub(crate) struct SimpleAggregateFunctionColumnData {
    pub(crate) inner: ArcColumnData,
    pub(crate) func: SimpleAggFunc,
}

impl SimpleAggregateFunctionColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        func: SimpleAggFunc,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let inner =
            <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, type_name, size, tz)?;
        Ok(SimpleAggregateFunctionColumnData { inner, func })
    }
}

impl ColumnData for SimpleAggregateFunctionColumnData {
    fn sql_type(&self) -> SqlType {
        let inner_type = self.inner.sql_type();
        SqlType::SimpleAggregateFunction(self.func, inner_type.into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        self.inner.save(encoder, start, end);
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&mut self, value: Value) {
        let inner_column: &mut dyn ColumnData = Arc::get_mut(&mut self.inner).unwrap();
        inner_column.push(value);
    }

    fn at(&self, index: usize) -> ValueRef {
        self.inner.at(index)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone(),
            func: self.func,
        })
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        props: u32,
    ) -> Result<()> {
        self.inner.get_internal(pointers, level, props)
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if let SqlType::SimpleAggregateFunction(func, inner_target) = target {
            if let Some(inner) = self.inner.cast_to(&self.inner, inner_target) {
                return Some(Arc::new(SimpleAggregateFunctionColumnData {
                    inner,
                    func: *func,
                }));
            }
        }
        None
    }

    fn get_timezone(&self) -> Option<Tz> {
        self.inner.get_timezone()
    }
}
