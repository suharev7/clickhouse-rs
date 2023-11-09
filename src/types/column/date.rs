use std::{fmt, ptr, sync::Arc};

use chrono::prelude::*;
use chrono_tz::Tz;
use either::Either;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            array::ArrayColumnData,
            column_data::{BoxColumnData, ColumnData, LowCardinalityAccessor},
            datetime64::DEFAULT_TZ,
            list::List,
            nullable::NullableColumnData,
            numeric::save_data,
            ArcColumnWrapper, ColumnFrom, ColumnWrapper,
        },
        DateConverter, Marshal, SqlType, StatBuffer, Unmarshal, Value, ValueRef,
    },
};

pub struct DateColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + Into<Value>
        + From<Value>
        + fmt::Display
        + Sync
        + Default
        + 'static,
{
    data: List<T>,
    tz: Tz,
}

impl<T> DateColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + Into<Value>
        + From<Value>
        + fmt::Display
        + Sync
        + Default
        + 'static,
{
    pub(crate) fn with_capacity(capacity: usize, timezone: Tz) -> DateColumnData<T> {
        DateColumnData {
            data: List::with_capacity(capacity),
            tz: timezone,
        }
    }

    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        size: usize,
        tz: Tz,
    ) -> Result<DateColumnData<T>> {
        let mut data = List::with_capacity(size);
        unsafe {
            data.set_len(size);
        }
        reader.read_bytes(data.as_mut())?;
        Ok(DateColumnData { data, tz })
    }
}

impl ColumnFrom for Vec<NaiveDate> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<u16>::with_capacity(source.len());
        for s in source {
            data.push(u16::get_days(s));
        }

        let column: DateColumnData<u16> = DateColumnData {
            data,
            tz: *DEFAULT_TZ,
        };
        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Vec<NaiveDate>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake: Vec<NaiveDate> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);
        let sql_type = inner.sql_type();

        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };

        for vs in source {
            let mut inner = Vec::with_capacity(vs.len());
            for v in vs {
                let days = u16::get_days(v);
                let value: Value = Value::Date(days);
                inner.push(value);
            }
            data.push(Value::Array(sql_type.clone().into(), Arc::new(inner)));
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Vec<DateTime<Tz>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake: Vec<DateTime<Tz>> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);
        let sql_type = inner.sql_type();

        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };

        for vs in source {
            let mut inner = Vec::with_capacity(vs.len());
            for v in vs {
                let value: Value = Value::DateTime(v.timestamp() as u32, v.timezone());
                inner.push(value);
            }
            data.push(Value::Array(sql_type.clone().into(), Arc::new(inner)));
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Option<NaiveDate>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let fake: Vec<NaiveDate> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);

        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };

        for value in source {
            match value {
                None => data.push(Value::Nullable(Either::Left(SqlType::Date.into()))),
                Some(d) => {
                    let days = u16::get_days(d);
                    let value = Value::Date(days);
                    data.push(Value::Nullable(Either::Right(Box::new(value))))
                }
            }
        }

        W::wrap(data)
    }
}

impl<T> LowCardinalityAccessor for DateColumnData<T> where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + Into<Value>
        + From<Value>
        + fmt::Display
        + Sync
        + Send
        + DateConverter
        + Default
        + 'static
{
}

impl<T> ColumnData for DateColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + Into<Value>
        + From<Value>
        + fmt::Display
        + Sync
        + Send
        + DateConverter
        + Default
        + 'static,
{
    fn sql_type(&self) -> SqlType {
        T::date_type()
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        save_data::<T>(self.data.as_ref(), encoder, start, end);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        self.data.push(T::get_stamp(value));
    }

    fn at(&self, index: usize) -> ValueRef {
        self.data.at(index).to_date(self.tz)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
            tz: self.tz,
        })
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        _props: u32,
    ) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = self.data.as_ptr() as *const u8;
        *pointers[1] = &self.tz as *const Tz as *const u8;
        *(pointers[2] as *mut usize) = self.len();
        Ok(())
    }

    unsafe fn get_internals(&self, data_ptr: *mut (), _level: u8, _props: u32) -> Result<()> {
        unsafe {
            let data_ref = &mut *(data_ptr as *mut DateTimeInternals);
            data_ref.begin = self.data.as_ptr().cast();
            data_ref.len = self.data.len();
            data_ref.tz = self.tz;
            Ok(())
        }
    }

    fn get_timezone(&self) -> Option<Tz> {
        Some(self.tz)
    }

    fn get_low_cardinality_accessor(&self) -> Option<&dyn LowCardinalityAccessor> {
        Some(self)
    }
}

#[derive(Debug)]
pub(crate) struct DateTimeInternals {
    pub(crate) begin: *const (),
    pub(crate) len: usize,
    pub(crate) tz: Tz,
    pub(crate) precision: Option<u32>,
}

impl Default for DateTimeInternals {
    fn default() -> Self {
        Self {
            begin: ptr::null(),
            len: 0,
            tz: Tz::Zulu,
            precision: None,
        }
    }
}

#[cfg(test)]
mod test {
    use chrono::TimeZone;

    use crate::types::column::{datetime64::DEFAULT_TZ, ArcColumnWrapper};

    use super::*;

    #[test]
    fn test_create_date() {
        let column =
            Vec::column_from::<ArcColumnWrapper>(vec![
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap()
            ]);
        assert_eq!("2016-10-22", format!("{:#}", column.at(0)));
        assert_eq!(SqlType::Date, column.sql_type());
    }

    #[test]
    fn test_create_date_time() {
        let tz = *DEFAULT_TZ;
        let column = Vec::column_from::<ArcColumnWrapper>(vec![tz
            .with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
            .unwrap()]);
        assert_eq!(format!("{}", column.at(0)), "2016-10-22 12:00:00");
    }
}
