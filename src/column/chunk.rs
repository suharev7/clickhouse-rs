use std::ops;

use binary::Encoder;
use column::{BoxColumnData, ColumnData};
use types::{SqlType, Value, ValueRef};

pub struct ChunkColumnData {
    data: BoxColumnData,
    range: ops::Range<usize>,
}

impl ChunkColumnData {
    pub fn new(data: BoxColumnData, range: ops::Range<usize>) -> ChunkColumnData {
        ChunkColumnData { data, range }
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
