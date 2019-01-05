use std::ops;

use crate::{
    binary::Encoder,
    types::{SqlType, Value, ValueRef},
};

use super::{BoxColumnData, ColumnData};

pub struct ChunkColumnData {
    data: BoxColumnData,
    range: ops::Range<usize>,
}

impl ChunkColumnData {
    pub fn new(data: BoxColumnData, range: ops::Range<usize>) -> Self {
        Self { data, range }
    }
}

impl ColumnData for ChunkColumnData {
    fn sql_type(&self) -> SqlType {
        self.data.sql_type()
    }

    fn save(&self, encoder: &mut Encoder) {
        for i in self.range.clone() {
            encoder.value(self.data.at(i))
        }
    }

    fn len(&self) -> usize {
        self.range.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        if index >= self.range.len() {
            panic!("out of range");
        }

        self.data.at(index + self.range.start)
    }
}
