use chrono_tz::Tz;
use std::{borrow::Cow, collections::HashMap, mem, ptr, sync::Arc};

use crate::{
    binary::{Encoder, ReadEx},
    errors::{DriverError, Error, FromSqlError, Result},
    types::{
        column::{
            column_data::{ArcColumnData, BoxColumnData, LowCardinalityAccessor},
            ArcColumnWrapper, ColumnData, VectorColumnData,
        },
        SqlType, Value, ValueRef,
    },
};

const NEED_GLOBAL_DICTIONARY_BIT: u64 = 1u64 << 8;
const HAS_ADDITIONAL_KEYS_BIT: u64 = 1u64 << 9;
const NEED_UPDATE_DICTIONARY_BIT: u64 = 1u64 << 10;

const TUINT8: u64 = 0;
const TUINT16: u64 = 1;
const TUINT32: u64 = 2;
const TUINT64: u64 = 3;

const LOW_CARDINALITY_VERSION: u64 = 1;
const INDEX_TYPE_MASK: u64 = 0xff;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum IndexType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

impl IndexType {
    fn from_flags(flags: u64) -> Result<Self> {
        match flags & INDEX_TYPE_MASK {
            TUINT8 => Ok(IndexType::UInt8),
            TUINT16 => Ok(IndexType::UInt16),
            TUINT32 => Ok(IndexType::UInt32),
            TUINT64 => Ok(IndexType::UInt64),
            _ => Err(Error::Driver(DriverError::Deserialize(Cow::from(
                "Invalid index serialization version value",
            )))),
        }
    }
}

#[derive(Clone)]
pub(crate) enum LowCardinalityIndex {
    UInt8(VectorColumnData<u8>),
    UInt16(VectorColumnData<u16>),
    UInt32(VectorColumnData<u32>),
    UInt64(VectorColumnData<u64>),
}

impl LowCardinalityIndex {
    fn get_flags(&self) -> u64 {
        // TODO NEED_UPDATE_DICTIONARY_BIT?
        match self {
            LowCardinalityIndex::UInt8(_) => {
                TUINT8 | HAS_ADDITIONAL_KEYS_BIT | NEED_UPDATE_DICTIONARY_BIT
            }
            LowCardinalityIndex::UInt16(_) => {
                TUINT16 | HAS_ADDITIONAL_KEYS_BIT | NEED_UPDATE_DICTIONARY_BIT
            }
            LowCardinalityIndex::UInt32(_) => {
                TUINT32 | HAS_ADDITIONAL_KEYS_BIT | NEED_UPDATE_DICTIONARY_BIT
            }
            LowCardinalityIndex::UInt64(_) => {
                TUINT64 | HAS_ADDITIONAL_KEYS_BIT | NEED_UPDATE_DICTIONARY_BIT
            }
        }
    }

    fn load<R: ReadEx>(
        reader: &mut R,
        size: usize,
        type_: IndexType,
    ) -> Result<LowCardinalityIndex> {
        Ok(match type_ {
            IndexType::UInt8 => {
                LowCardinalityIndex::UInt8(VectorColumnData::<u8>::load(reader, size)?)
            }
            IndexType::UInt16 => {
                LowCardinalityIndex::UInt16(VectorColumnData::<u16>::load(reader, size)?)
            }
            IndexType::UInt32 => {
                LowCardinalityIndex::UInt32(VectorColumnData::<u32>::load(reader, size)?)
            }
            IndexType::UInt64 => {
                LowCardinalityIndex::UInt64(VectorColumnData::<u64>::load(reader, size)?)
            }
        })
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            LowCardinalityIndex::UInt8(ix) => ix.len(),
            LowCardinalityIndex::UInt16(ix) => ix.len(),
            LowCardinalityIndex::UInt32(ix) => ix.len(),
            LowCardinalityIndex::UInt64(ix) => ix.len(),
        }
    }

    pub(crate) fn get_by_index(&self, index: usize) -> usize {
        match self {
            LowCardinalityIndex::UInt8(ix) => ix.get_by_index(index) as usize,
            LowCardinalityIndex::UInt16(ix) => ix.get_by_index(index) as usize,
            LowCardinalityIndex::UInt32(ix) => ix.get_by_index(index) as usize,
            LowCardinalityIndex::UInt64(ix) => ix.get_by_index(index) as usize,
        }
    }

    fn max_len(&self) -> usize {
        match self {
            LowCardinalityIndex::UInt8(_) => u8::MAX as usize,
            LowCardinalityIndex::UInt16(_) => u16::MAX as usize,
            LowCardinalityIndex::UInt32(_) => u32::MAX as usize,
            LowCardinalityIndex::UInt64(_) => u64::MAX as usize,
        }
    }

    fn inc_max_len(&self) -> Self {
        match self {
            LowCardinalityIndex::UInt8(u) => {
                let data = u.data.map(|v| v as u16);
                LowCardinalityIndex::UInt16(VectorColumnData { data })
            }
            LowCardinalityIndex::UInt16(u) => {
                let data = u.data.map(|v| v as u32);
                LowCardinalityIndex::UInt32(VectorColumnData { data })
            }
            LowCardinalityIndex::UInt32(u) => {
                let data = u.data.map(|v| v as u64);
                LowCardinalityIndex::UInt64(VectorColumnData { data })
            }
            LowCardinalityIndex::UInt64(_) => {
                unreachable!()
            }
        }
    }

    fn push(&mut self, value: usize) {
        if self.len() + 1 > self.max_len() {
            *self = self.inc_max_len();
        }
        match self {
            LowCardinalityIndex::UInt8(u) => u.data.push(value as u8),
            LowCardinalityIndex::UInt16(u) => u.data.push(value as u16),
            LowCardinalityIndex::UInt32(u) => u.data.push(value as u32),
            LowCardinalityIndex::UInt64(u) => u.data.push(value as u64),
        }
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        match self {
            LowCardinalityIndex::UInt8(s) => s.save(encoder, start, end),
            LowCardinalityIndex::UInt16(s) => s.save(encoder, start, end),
            LowCardinalityIndex::UInt32(s) => s.save(encoder, start, end),
            LowCardinalityIndex::UInt64(s) => s.save(encoder, start, end),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LowCardinalityInternals {
    pub(crate) index: *const LowCardinalityIndex,
    pub(crate) accessor: *const dyn LowCardinalityAccessor,
}

impl Default for LowCardinalityInternals {
    fn default() -> Self {
        LowCardinalityInternals {
            index: ptr::null_mut(),
            accessor: unsafe { mem::transmute([ptr::null_mut::<()>(), ptr::null_mut()]) },
        }
    }
}

pub(crate) struct LowCardinalityColumnData {
    pub(crate) inner: ArcColumnData,
    pub(crate) index: LowCardinalityIndex,
    pub(crate) value_map: Option<HashMap<Value, usize>>,
}

impl LowCardinalityColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        inner_type: &str,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let (inner, index) = read_inner(reader, inner_type, size, tz)?;
        Ok(LowCardinalityColumnData {
            inner,
            index,
            value_map: None,
        })
    }

    pub(crate) fn empty(
        inner: &SqlType,
        timezone: Tz,
        capacity: usize,
    ) -> Result<LowCardinalityColumnData> {
        Ok(LowCardinalityColumnData {
            inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                inner.clone(),
                timezone,
                capacity,
            )?,
            index: LowCardinalityIndex::UInt8(VectorColumnData::<u8>::with_capacity(capacity)),
            value_map: None,
        })
    }
}

fn read_inner<R: ReadEx>(
    reader: &mut R,
    inner_type: &str,
    size: usize,
    tz: Tz,
) -> Result<(ArcColumnData, LowCardinalityIndex)> {
    if size == 0 {
        let inner =
            <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, inner_type, size, tz)?;
        let keys = LowCardinalityIndex::load(reader, size, IndexType::UInt8)?;
        return Ok((inner, keys));
    }

    read_prefix(reader)?;
    let flags: u64 = reader.read_scalar()?;
    let index_type = IndexType::from_flags(flags)?;

    if flags & NEED_GLOBAL_DICTIONARY_BIT != 0 {
        return Err(Error::Driver(DriverError::Deserialize(Cow::from(
            "Global dictionary is not supported.",
        ))));
    }

    if flags & HAS_ADDITIONAL_KEYS_BIT == 0 {
        return Err(Error::Driver(DriverError::Deserialize(Cow::from(
            "HasAdditionalKeysBit is missing.",
        ))));
    }

    let new_index_column: u64 = reader.read_scalar()?;
    let inner = <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(
        reader,
        inner_type,
        new_index_column as usize,
        tz,
    )?;

    let keys_rows: u64 = reader.read_scalar()?;
    let keys = LowCardinalityIndex::load(reader, keys_rows as usize, index_type)?;
    assert_eq!(flags, keys.get_flags());

    Ok((inner, keys))
}

fn read_prefix<R: ReadEx>(reader: &mut R) -> Result<()> {
    let version: u64 = reader.read_scalar()?;
    if version != LOW_CARDINALITY_VERSION {
        return Err(Error::Driver(DriverError::Deserialize(Cow::from(
            "Invalid low cardinality version",
        ))));
    }
    Ok(())
}

impl LowCardinalityColumnData {
    fn get_value_map(&self) -> HashMap<Value, usize> {
        (0..self.index.len())
            .map(|index| {
                let value_index = self.index.get_by_index(index);
                let value: Value = self.inner.at(value_index).into();
                (value, value_index)
            })
            .collect()
    }
}

impl ColumnData for LowCardinalityColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::LowCardinality(self.inner.sql_type().into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        if start == end {
            return;
        }

        encoder.write(LOW_CARDINALITY_VERSION);
        encoder.write(self.index.get_flags());

        encoder.write(self.inner.len() as u64);
        self.inner.save(encoder, 0, self.inner.len());

        encoder.write((end - start) as u64);
        self.index.save(encoder, start, end)
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn push(&mut self, value: Value) {
        let value_map = loop {
            match self.value_map {
                None => self.value_map = Some(self.get_value_map()),
                Some(ref mut value_map) => break value_map,
            };
        };

        match value_map.get_mut(&value) {
            None => {
                let new_index = self.inner.len();
                Arc::get_mut(&mut self.inner)
                    .expect("Column shouldn't be shared")
                    .push(value.clone());
                value_map.insert(value, new_index);
                self.index.push(new_index);
            }
            Some(index) => self.index.push(*index),
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let ix = self.index.get_by_index(index);
        self.inner.at(ix)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone(),
            index: self.index.clone(),
            value_map: None,
        })
    }

    fn get_timezone(&self) -> Option<Tz> {
        self.inner.get_timezone()
    }

    unsafe fn get_internals(&self, data_ptr: *mut (), level: u8, props: u32) -> Result<()> {
        if level != 1 {
            return self.inner.get_internals(data_ptr, level, props);
        }
        let Some(accessor) = self.inner.get_low_cardinality_accessor() else {
            return Err(Error::FromSql(FromSqlError::UnsupportedOperation));
        };
        unsafe {
            let data_ref = &mut *(data_ptr as *mut LowCardinalityInternals);
            data_ref.index = &self.index as *const LowCardinalityIndex;
            data_ref.accessor = mem::transmute(accessor);
            Ok(())
        }
    }
}
