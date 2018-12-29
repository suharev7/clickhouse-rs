use std::ops;
use std::sync::Arc;

use chrono_tz::Tz;

use crate::binary::{Encoder, ReadEx};
use crate::column::chunk::ChunkColumnData;
use crate::types::{ClickhouseResult, SqlType, ValueRef};

pub use self::column_data::{ColumnData, ColumnDataExt};
pub use self::concat::ConcatColumnData;
pub use self::numeric::VectorColumnData;
pub use self::string::StringColumnData;

mod chunk;
mod column_data;
mod concat;
mod date;
mod factory;
mod list;
mod numeric;
mod string;

pub type BoxColumnData = Arc<ColumnData + Send + Sync>;

pub struct Column {
    name: String,
    data: BoxColumnData,
}

pub trait ColumnFrom {
    fn column_from(source: Self) -> BoxColumnData;
}

impl ColumnFrom for Column {
    fn column_from(source: Column) -> BoxColumnData {
        source.data
    }
}

impl PartialEq<Column> for Column {
    fn eq(&self, other: &Column) -> bool {
        if self.len() != other.len() {
            return false;
        }

        if self.sql_type() != other.sql_type() {
            return false;
        }

        for i in 0..self.len() {
            if self.at(i) != other.at(i) {
                return false;
            }
        }

        return true;
    }
}

impl Clone for Column {
    fn clone(&self) -> Self {
        Column {
            name: self.name.clone(),
            data: self.data.clone(),
        }
    }
}

impl Column {
    pub fn read<R: ReadEx>(reader: &mut R, size: usize, tz: Tz) -> ClickhouseResult<Column> {
        let name = reader.read_string()?;
        let type_name = reader.read_string()?;
        let data = ColumnData::load_data(reader, &type_name, size, tz)?;
        let column = Column { name, data };
        Ok(column)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sql_type(&self) -> SqlType {
        self.data.sql_type()
    }

    pub fn at(&self, index: usize) -> ValueRef {
        self.data.at(index)
    }

    pub fn write(&self, encoder: &mut Encoder) {
        encoder.string(&self.name);
        encoder.string(self.data.sql_type().to_string().as_ref());
        self.data.save(encoder);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn concat<'a, I>(items: I) -> Column
    where
        I: Iterator<Item = &'a Column>,
    {
        let items_vec: Vec<&Column> = items.collect();
        let chunks: Vec<_> = items_vec.iter().map(|column| column.data.clone()).collect();
        match items_vec.first() {
            None => unreachable!(),
            Some(ref first_column) => {
                let name: String = first_column.name().to_string();
                let data = ConcatColumnData::concat(chunks);
                Column {
                    name,
                    data: Arc::new(data),
                }
            }
        }
    }

    pub fn slice(&self, range: ops::Range<usize>) -> Column {
        let data = ChunkColumnData::new(self.data.clone(), range);
        Column {
            name: self.name.clone(),
            data: Arc::new(data),
        }
    }
}

pub fn new_column(name: &str, data: Arc<(ColumnData + Sync + Send + 'static)>) -> Column {
    Column {
        name: name.to_string(),
        data,
    }
}
