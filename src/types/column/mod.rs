use std::{ops, fmt, sync::Arc};

use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::{Error, FromSqlError, Result},
    types::{
        column::{
            decimal::{DecimalAdapter, NullableDecimalAdapter},
            fixed_string::{FixedStringAdapter, NullableFixedStringAdapter},
            string::StringAdapter, column_data::ArcColumnData
        },
        decimal::NoBits, SqlType, ValueRef, Value
    },
};

pub use self::{
    concat::ConcatColumnData,
    numeric::VectorColumnData,
};
use self::chunk::ChunkColumnData;
pub(crate) use self::{string_pool::StringPool, column_data::ColumnData};

mod array;
mod chunk;
mod column_data;
mod concat;
mod date;
mod decimal;
mod factory;
pub(crate) mod fixed_string;
mod list;
mod nullable;
mod numeric;
mod string;
mod string_pool;

/// Represents Clickhouse Column
pub struct Column {
    pub(crate) name: String,
    pub(crate) data: ArcColumnData,
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
    pub(crate) fn read<R: ReadEx>(reader: &mut R, size: usize, tz: Tz) -> Result<Column> {
        let name = reader.read_string()?;
        let type_name = reader.read_string()?;
        let data = ColumnData::load_data::<ArcColumnWrapper, _>(reader, &type_name, size, tz)?;
        let column = Self { name, data };
        Ok(column)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    pub fn sql_type(&self) -> SqlType {
        self.data.sql_type()
    }

    #[inline(always)]
    pub(crate) fn at(&self, index: usize) -> ValueRef {
        self.data.at(index)
    }

    pub(crate) fn write(&self, encoder: &mut Encoder) {
        encoder.string(&self.name);
        encoder.string(self.data.sql_type().to_string().as_ref());
        let len = self.data.len();
        self.data.save(encoder, 0, len);
    }

    #[inline(always)]
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

    pub fn cast_to(self, dst_type: SqlType) -> Result<Column> {
        let src_type = self.sql_type();

        if dst_type == src_type {
            return Ok(self);
        }

        match (dst_type, src_type) {
            (SqlType::FixedString(str_len), SqlType::String) => {
                let name = self.name().to_owned();
                let adapter = FixedStringAdapter {
                    column: self,
                    str_len,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                })
            }
            (
                SqlType::Nullable(SqlType::FixedString(str_len)),
                SqlType::Nullable(SqlType::String),
            ) => {
                let name = self.name().to_owned();
                let adapter = NullableFixedStringAdapter {
                    column: self,
                    str_len: *str_len,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                })
            }
            (SqlType::String, SqlType::Array(SqlType::UInt8)) => {
                let name = self.name().to_owned();
                let adapter = StringAdapter { column: self };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                })
            }
            (SqlType::FixedString(n), SqlType::Array(SqlType::UInt8)) => {
                let string_column = self.cast_to(SqlType::String)?;
                string_column.cast_to(SqlType::FixedString(n))
            }
            (SqlType::Decimal(dst_p, dst_s), SqlType::Decimal(_, _)) => {
                let name = self.name().to_owned();
                let nobits = NoBits::from_precision(dst_p).unwrap();
                let adapter = DecimalAdapter {
                    column: self,
                    precision: dst_p,
                    scale: dst_s,
                    nobits,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                })
            }
            (
                SqlType::Nullable(SqlType::Decimal(dst_p, dst_s)),
                SqlType::Nullable(SqlType::Decimal(_, _)),
            ) => {
                let name = self.name().to_owned();
                let nobits = NoBits::from_precision(*dst_p).unwrap();
                let adapter = NullableDecimalAdapter {
                    column: self,
                    precision: *dst_p,
                    scale: *dst_s,
                    nobits,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                })
            }
            _ => Err(Error::FromSql(FromSqlError::InvalidType {
                src: src_type.to_string(),
                dst: dst_type.to_string(),
            })),
        }
    }

    pub(crate) fn push(&mut self, value: Value) {
        loop {
            match Arc::get_mut(&mut self.data) {
                None => {
                    self.data = Arc::from(self.data.clone_instance());
                },
                Some(data) => {
                    data.push(value);
                    break;
                },
            }
        }
    }

    pub(crate) unsafe fn as_ptr(&self) -> Result<*const u8> {
        self.data.as_ptr()
    }
}

pub(crate) fn new_column(name: &str, data: Arc<(dyn ColumnData + Sync + Send + 'static)>) -> Column {
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

pub(crate) struct ArcColumnWrapper {
    _private: (),
}

impl ColumnWrapper for ArcColumnWrapper {
    type Wrapper = Arc<dyn ColumnData + Send + Sync>;

    fn wrap<T: ColumnData + Send + Sync + 'static>(column: T) -> Self::Wrapper {
        Arc::new(column)
    }

    fn wrap_arc(data: ArcColumnData) -> Self::Wrapper {
        data
    }
}

pub(crate) struct BoxColumnWrapper {
    _private: (),
}

impl ColumnWrapper for BoxColumnWrapper {
    type Wrapper = Box<dyn ColumnData + Send + Sync>;

    fn wrap<T: ColumnData + Send + Sync + 'static>(column: T) -> Self::Wrapper {
        Box::new(column)
    }

    fn wrap_arc(_: ArcColumnData) -> Self::Wrapper {
        unimplemented!()
    }
}
