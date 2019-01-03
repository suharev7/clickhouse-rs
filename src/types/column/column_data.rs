use std::convert;

use crate::{
    binary::Encoder,
    types::{SqlType, Value, ValueRef}
};

pub trait ColumnData {
    fn sql_type(&self) -> SqlType;
    fn save(&self, encoder: &mut Encoder);
    fn len(&self) -> usize;
    fn push(&mut self, value: Value);
    fn at(&self, index: usize) -> ValueRef;
}

pub trait ColumnDataExt {
    fn append<T: convert::Into<Value>>(&mut self, value: T);
}

impl<C: ColumnData> ColumnDataExt for C {
    fn append<T: convert::Into<Value>>(&mut self, value: T) {
        self.push(value.into());
    }
}
