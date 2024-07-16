use std::{
    marker,
    net::{Ipv4Addr, Ipv6Addr},
    ops,
    sync::Arc,
};

use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::{Error, FromSqlError, Result},
    types::{
        column::{
            column_data::ArcColumnData,
            decimal::{DecimalAdapter, NullableDecimalAdapter},
            enums::{Enum16Adapter, Enum8Adapter, NullableEnum16Adapter, NullableEnum8Adapter},
            fixed_string::{FixedStringAdapter, NullableFixedStringAdapter},
            ip::{IpColumnData, Ipv4, Ipv6},
            iter::Iterable,
            low_cardinality::LowCardinalityColumnData,
            simple_agg_func::SimpleAggregateFunctionColumnData,
            string::StringAdapter,
        },
        decimal::NoBits,
        SqlType, Value, ValueRef,
    },
};

use self::chunk::ChunkColumnData;
pub(crate) use self::string_pool::StringPool;
pub use self::column_data::ColumnData;
pub use self::{concat::ConcatColumnData, numeric::VectorColumnData};

mod array;
pub(crate) mod chrono_datetime;
mod chunk;
mod column_data;
mod concat;
mod date;
pub(crate) mod datetime64;
mod decimal;
mod enums;
mod factory;
pub(crate) mod fixed_string;
mod ip;
pub mod iter;
mod list;
mod low_cardinality;
mod map;
mod nullable;
mod numeric;
mod simple_agg_func;
mod string;
mod string_pool;
mod util;

/// Represents Clickhouse Column
pub struct Column<K: ColumnType> {
    pub(crate) name: String,
    pub(crate) data: ArcColumnData,
    pub(crate) _marker: marker::PhantomData<K>,
}

pub trait ColumnFrom {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper;
}

pub trait ColumnType: Send + Copy + Sync + 'static {}

#[derive(Copy, Clone, Default)]
pub struct Simple {
    _private: (),
}

#[derive(Copy, Clone, Default)]
pub struct Complex {
    _private: (),
}

impl ColumnType for Simple {}

impl ColumnType for Complex {}

impl<K: ColumnType> ColumnFrom for Column<K> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        W::wrap_arc(source.data)
    }
}

impl<L: ColumnType, R: ColumnType> PartialEq<Column<R>> for Column<L> {
    fn eq(&self, other: &Column<R>) -> bool {
        if self.len() != other.len() || self.sql_type() != other.sql_type() {
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

impl<K: ColumnType> Clone for Column<K> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            data: self.data.clone(),
            _marker: marker::PhantomData,
        }
    }
}

impl Column<Simple> {
    pub(crate) fn concat<'a, I>(items: I) -> Column<Complex>
    where
        I: Iterator<Item = &'a Self>,
    {
        let items_vec: Vec<&Self> = items.collect();
        let chunks: Vec<_> = items_vec.iter().map(|column| column.data.clone()).collect();
        match items_vec.first() {
            None => unreachable!(),
            Some(first_column) => {
                let name: String = first_column.name().to_string();
                let data = ConcatColumnData::concat(chunks);
                Column {
                    name,
                    data: Arc::new(data),
                    _marker: marker::PhantomData,
                }
            }
        }
    }
}

impl<K: ColumnType> Column<K> {
    /// Returns an iterator over the column.
    ///
    /// ### Example
    ///
    /// ```rust
    /// # use std::env;
    /// # use clickhouse_rs::{errors::Error, Pool, errors::Result};
    /// # use futures_util::stream::StreamExt;
    /// # let mut rt = tokio::runtime::Runtime::new().unwrap();
    /// # let ret: Result<()> = rt.block_on(async {
    /// #     let database_url = "tcp://localhost:9000";
    /// #     let pool = Pool::new(database_url);
    /// #     let mut client = pool.get_handle().await?;
    ///       let mut stream = client
    ///             .query("SELECT number as n1, number as n2, number as n3 FROM numbers(1000)")
    ///             .stream_blocks();
    ///
    ///       let mut sum = 0;
    ///       while let Some(block) = stream.next().await {
    ///           let block = block?;
    ///
    ///           let c1 = block.get_column("n1")?.iter::<u64>()?;
    ///           let c2 = block.get_column("n2")?.iter::<u64>()?;
    ///           let c3 = block.get_column("n3")?.iter::<u64>()?;
    ///
    ///           for ((v1, v2), v3) in c1.zip(c2).zip(c3) {
    ///               sum = v1 + v2 + v3;
    ///           }
    ///       }
    ///
    ///       dbg!(sum);
    /// #     Ok(())
    /// # });
    /// # ret.unwrap()
    /// ```
    pub fn iter<'a, T: Iterable<'a, K>>(&'a self) -> Result<T::Iter> {
        <T as Iterable<'a, K>>::iter(self, self.sql_type())
    }
}

impl<K: ColumnType> Column<K> {
    pub(crate) fn read<R: ReadEx>(reader: &mut R, size: usize, tz: Tz) -> Result<Column<K>> {
        let name = reader.read_string()?;
        let type_name = reader.read_string()?;
        let data =
            <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, &type_name, size, tz)?;
        let column = Self {
            name,
            data,
            _marker: marker::PhantomData,
        };
        Ok(column)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    pub fn data(&self) -> ArcColumnData {
        self.data.clone()
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

    pub(crate) fn slice(&self, range: ops::Range<usize>) -> Column<Complex> {
        let data = ChunkColumnData::new(self.data.clone(), range);
        Column {
            name: self.name.clone(),
            data: Arc::new(data),
            _marker: marker::PhantomData,
        }
    }

    pub(crate) fn cast_to(self, dst_type: SqlType) -> Result<Self> {
        let src_type = self.sql_type();

        if dst_type == src_type {
            return Ok(self);
        }

        match (dst_type.clone(), src_type.clone()) {
            (SqlType::LowCardinality(inner), src_type) if src_type.is_inner_low_cardinality() => {
                let name = self.name().to_owned();
                let tz = self.data.get_timezone().unwrap_or(Tz::Zulu);
                let mut low_card_data = LowCardinalityColumnData::empty(inner, tz, self.len())?;
                for i in 0..self.len() {
                    low_card_data.push(self.at(i).into());
                }

                if inner.is_datetime() {
                    if let Some(casted_data) =
                        low_card_data.inner.cast_to(&low_card_data.inner, inner)
                    {
                        low_card_data.inner = casted_data;
                    }
                }

                Ok(Column {
                    name,
                    data: Arc::new(low_card_data),
                    _marker: marker::PhantomData,
                })
            }
            (SqlType::FixedString(str_len), SqlType::String) => {
                let name = self.name().to_owned();
                let adapter = FixedStringAdapter {
                    column: self,
                    str_len,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                    _marker: marker::PhantomData,
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
                    _marker: marker::PhantomData,
                })
            }
            (SqlType::String, SqlType::Array(SqlType::UInt8)) => {
                let name = self.name().to_owned();
                let adapter = StringAdapter { column: self };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                    _marker: marker::PhantomData,
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
                    _marker: marker::PhantomData,
                })
            }
            (SqlType::Enum8(enum_values), SqlType::Enum8(_)) => {
                let name = self.name().to_owned();
                let adapter = Enum8Adapter {
                    column: self,
                    enum_values,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                    _marker: marker::PhantomData,
                })
            }
            (SqlType::Enum16(enum_values), SqlType::Enum16(_)) => {
                let name = self.name().to_owned();
                let adapter = Enum16Adapter {
                    column: self,
                    enum_values,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                    _marker: marker::PhantomData,
                })
            }
            (
                SqlType::Nullable(SqlType::Enum8(enum_values)),
                SqlType::Nullable(SqlType::Enum8(_)),
            ) => {
                let name = self.name().to_owned();
                let enum_values = enum_values.clone();
                let adapter = NullableEnum8Adapter {
                    column: self,
                    enum_values,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                    _marker: marker::PhantomData,
                })
            }
            (
                SqlType::Nullable(SqlType::Enum16(enum_values)),
                SqlType::Nullable(SqlType::Enum16(_)),
            ) => {
                let name = self.name().to_owned();
                let enum_values = enum_values.clone();
                let adapter = NullableEnum16Adapter {
                    column: self,
                    enum_values,
                };
                Ok(Column {
                    name,
                    data: Arc::new(adapter),
                    _marker: marker::PhantomData,
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
                    _marker: marker::PhantomData,
                })
            }
            (SqlType::Ipv4, SqlType::String) => {
                let name = self.name().to_owned();

                let n = self.len();
                let mut inner = Vec::with_capacity(n);
                for i in 0..n {
                    let source = self.at(i).as_str().unwrap();
                    let ip: Ipv4Addr = source.parse().unwrap();
                    let mut buffer = [0_u8; 4];
                    buffer.copy_from_slice(&ip.octets());
                    buffer.reverse();
                    inner.extend(&buffer);
                }

                let data = Arc::new(IpColumnData::<Ipv4> {
                    inner,
                    phantom: marker::PhantomData,
                });

                Ok(Column {
                    name,
                    data,
                    _marker: marker::PhantomData,
                })
            }
            (SqlType::Ipv6, SqlType::String) => {
                let name = self.name().to_owned();

                let n = self.len();
                let mut inner = Vec::with_capacity(n);
                for i in 0..n {
                    let source = self.at(i).as_str().unwrap();
                    let ip: Ipv6Addr = source.parse().unwrap();
                    inner.extend(&ip.octets());
                }

                let data = Arc::new(IpColumnData::<Ipv6> {
                    inner,
                    phantom: marker::PhantomData,
                });

                Ok(Column {
                    name,
                    data,
                    _marker: marker::PhantomData,
                })
            }
            (SqlType::SimpleAggregateFunction(func, nested), _) => {
                let inner_column = self.cast_to(nested.clone())?;
                Ok(Column {
                    name: inner_column.name,
                    data: Arc::new(SimpleAggregateFunctionColumnData {
                        inner: inner_column.data,
                        func,
                    }),
                    _marker: marker::PhantomData,
                })
            }
            _ => {
                if let Some(data) = self.data.cast_to(&self.data, &dst_type) {
                    let name = self.name().to_owned();
                    Ok(Column {
                        name,
                        data,
                        _marker: marker::PhantomData,
                    })
                } else {
                    Err(Error::FromSql(FromSqlError::InvalidType {
                        src: src_type.to_string(),
                        dst: dst_type.to_string(),
                    }))
                }
            }
        }
    }

    pub(crate) fn push(&mut self, value: Value) {
        loop {
            match Arc::get_mut(&mut self.data) {
                None => {
                    self.data = Arc::from(self.data.clone_instance());
                }
                Some(data) => {
                    data.push(value);
                    break;
                }
            }
        }
    }

    pub(crate) unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        props: u32,
    ) -> Result<()> {
        self.data.get_internal(pointers, level, props)
    }

    pub(crate) unsafe fn get_internals<D>(
        &self,
        data: &mut D,
        level: u8,
        props: u32,
    ) -> Result<()> {
        self.data
            .get_internals(data as *mut D as *mut (), level, props)
    }
}

pub(crate) fn new_column<K: ColumnType>(
    name: &str,
    data: Arc<(dyn ColumnData + Sync + Send + 'static)>,
) -> Column<K> {
    Column {
        name: name.to_string(),
        data,
        _marker: marker::PhantomData,
    }
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
