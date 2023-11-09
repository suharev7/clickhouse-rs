use chrono::{prelude::*, LocalResult};
use chrono_tz::Tz;
use lazy_static::lazy_static;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            column_data::{BoxColumnData, ColumnData},
            date::DateTimeInternals,
            list::List,
        },
        DateTimeType, SqlType, Value, ValueRef,
    },
};

lazy_static! {
    pub(crate) static ref DEFAULT_TZ: Tz = Tz::Zulu;
}

pub struct DateTime64ColumnData {
    data: List<i64>,
    params: (u32, Tz),
}

impl DateTime64ColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        size: usize,
        precision: u32,
        tz: Tz,
    ) -> Result<DateTime64ColumnData> {
        let mut data = List::with_capacity(size);
        unsafe {
            data.set_len(size);
        }
        reader.read_bytes(data.as_mut())?;
        Ok(DateTime64ColumnData {
            data,
            params: (precision, tz),
        })
    }

    pub(crate) fn with_capacity(capacity: usize, precision: u32, timezone: Tz) -> Self {
        DateTime64ColumnData {
            data: List::with_capacity(capacity),
            params: (precision, timezone),
        }
    }
}

impl ColumnData for DateTime64ColumnData {
    fn sql_type(&self) -> SqlType {
        let (precision, tz) = self.params;
        SqlType::DateTime(DateTimeType::DateTime64(precision, tz))
    }

    fn save(&self, _encoder: &mut Encoder, _start: usize, _end: usize) {
        unimplemented!()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        let (precision, tz) = &self.params;
        let time = DateTime::<Tz>::from(value);
        let stamp = from_datetime(time.with_timezone(tz), *precision);
        self.data.push(stamp)
    }

    fn at(&self, index: usize) -> ValueRef {
        let value = self.data.at(index);
        ValueRef::DateTime64(value, &self.params)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
            params: self.params,
        })
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        _props: u32,
    ) -> Result<()> {
        assert_eq!(level, 0);
        let (precision, tz) = &self.params;
        *pointers[0] = self.data.as_ptr() as *const u8;
        *pointers[1] = tz as *const Tz as *const u8;
        *(pointers[2] as *mut usize) = self.len();
        *(pointers[3] as *mut Option<u32>) = Some(*precision);
        Ok(())
    }

    unsafe fn get_internals(&self, data_ptr: *mut (), _level: u8, _props: u32) -> Result<()> {
        let (precision, tz) = &self.params;
        unsafe {
            let data_ref = &mut *(data_ptr as *mut DateTimeInternals);
            data_ref.begin = self.data.as_ptr().cast();
            data_ref.len = self.data.len();
            data_ref.tz = *tz;
            data_ref.precision = Some(*precision);
            Ok(())
        }
    }

    fn get_timezone(&self) -> Option<Tz> {
        let (_, tz) = self.params;
        Some(tz)
    }
}

pub(crate) fn from_datetime<T: TimeZone>(time: DateTime<T>, precision: u32) -> i64 {
    let base10: i64 = 10;
    let timestamp = time.timestamp_nanos_opt().unwrap();
    timestamp / base10.pow(9 - precision)
}

#[inline(always)]
pub(crate) fn to_datetime(value: i64, precision: u32, tz: Tz) -> DateTime<Tz> {
    to_datetime_opt(value, precision, tz).unwrap()
}

#[inline(always)]
pub(crate) fn to_datetime_opt(value: i64, precision: u32, tz: Tz) -> LocalResult<DateTime<Tz>> {
    let base10: i64 = 10;

    let nano = if precision < 19 {
        value * base10.pow(9 - precision)
    } else {
        0_i64
    };

    let sec = nano / 1_000_000_000;
    let nsec = nano - sec * 1_000_000_000;

    tz.timestamp_opt(sec, nsec as u32)
}

#[inline(always)]
pub(crate) fn to_native_datetime_opt(value: i64, precision: u32) -> Option<NaiveDateTime> {
    let base10: i64 = 10;

    let nano = if precision < 19 {
        value * base10.pow(9 - precision)
    } else {
        0_i64
    };

    let sec = nano / 1_000_000_000;
    let nsec = nano - sec * 1_000_000_000;

    NaiveDateTime::from_timestamp_opt(sec, nsec as u32)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_to_datetime() {
        let expected = DateTime::parse_from_rfc3339("2019-01-01T00:00:00-00:00").unwrap();
        let actual = to_datetime(1_546_300_800_000, 3, Tz::UTC);
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_from_datetime() {
        let origin = DateTime::parse_from_rfc3339("2019-01-01T00:00:00-00:00").unwrap();
        let actual = from_datetime(origin, 3);
        assert_eq!(actual, 1_546_300_800_000)
    }
}
