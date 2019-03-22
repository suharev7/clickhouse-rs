use std::{ops, sync::Arc};

use crate::{
    binary::{Encoder, ReadEx},
    types::{ClickhouseResult, SqlType, ValueRef},
};
use chrono_tz::Tz;
use std::fmt;

use self::chunk::ChunkColumnData;
pub use self::{
    column_data::ColumnData, concat::ConcatColumnData, numeric::VectorColumnData,
    string::StringColumnData,
};

pub(crate) use self::string_pool::StringPool;

mod chunk;
mod column_data;
mod concat;
mod date;
mod factory;
mod list;
mod nullable;
mod numeric;
mod string;
mod string_pool;

pub type ArcColumnData = Arc<dyn ColumnData + Send + Sync>;

/// Represents Clickhouse Column
pub struct Column {
    name: String,
    data: ArcColumnData,
}

pub trait ColumnFrom {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper;
}

impl ColumnFrom for Column {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        W::wrap_arc(source.data)
    }
}

impl PartialEq<Column> for Column {
    fn eq(&self, other: &Self) -> bool {
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

        true
    }
}

impl Clone for Column {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            data: self.data.clone(),
        }
    }
}

impl Column {
    pub(crate) fn read<R: ReadEx>(reader: &mut R, size: usize, tz: Tz) -> ClickhouseResult<Column> {
        let name = reader.read_string()?;
        let type_name = reader.read_string()?;
        let data = ColumnData::load_data::<ArcColumnWrapper, _>(reader, &type_name, size, tz)?;
        let column = Self { name, data };
        Ok(column)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sql_type(&self) -> SqlType {
        self.data.sql_type()
    }

    pub(crate) fn at(&self, index: usize) -> ValueRef {
        self.data.at(index)
    }

    pub(crate) fn write(&self, encoder: &mut Encoder) {
        encoder.string(&self.name);
        encoder.string(self.data.sql_type().to_string().as_ref());
        let len = self.data.len();
        self.data.save(encoder, 0, len);
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn concat<'a, I>(items: I) -> Self
    where
        I: Iterator<Item = &'a Self>,
    {
        let items_vec: Vec<&Self> = items.collect();
        let chunks: Vec<_> = items_vec.iter().map(|column| column.data.clone()).collect();
        match items_vec.first() {
            None => unreachable!(),
            Some(ref first_column) => {
                let name: String = first_column.name().to_string();
                let data = ConcatColumnData::concat(chunks);
                Self {
                    name,
                    data: Arc::new(data),
                }
            }
        }
    }

    pub(crate) fn slice(&self, range: ops::Range<usize>) -> Self {
        let data = ChunkColumnData::new(self.data.clone(), range);
        Self {
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

#[derive(Clone, Debug, PartialEq)]
pub enum Either<L, R>
where
    L: fmt::Debug + PartialEq + Clone,
    R: fmt::Debug + PartialEq + Clone,
{
    Left(L),
    Right(R),
}

pub trait ColumnWrapper {
    type Wrapper;
    fn wrap<T: ColumnData + Send + Sync + 'static>(column: T) -> Self::Wrapper;

    fn wrap_arc(data: ArcColumnData) -> Self::Wrapper;
}

pub(crate) struct ArcColumnWrapper {}

impl ColumnWrapper for ArcColumnWrapper {
    type Wrapper = Arc<dyn ColumnData + Send + Sync>;

    fn wrap<T: ColumnData + Send + Sync + 'static>(column: T) -> Self::Wrapper {
        Arc::new(column)
    }

    fn wrap_arc(data: ArcColumnData) -> Self::Wrapper {
        data
    }
}

pub(crate) struct BoxColumnWrapper {}

impl ColumnWrapper for BoxColumnWrapper {
    type Wrapper = Box<dyn ColumnData + Send + Sync>;

    fn wrap<T: ColumnData + Send + Sync + 'static>(column: T) -> Self::Wrapper {
        Box::new(column)
    }

    fn wrap_arc(_: ArcColumnData) -> Self::Wrapper {
        unimplemented!()
    }
}
