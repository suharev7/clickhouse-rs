use std::{convert, fmt, mem};

use chrono::{prelude::*, Date};
use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Error,
    types::{
        column::{nullable::NullableColumnData, BoxColumnWrapper, ColumnWrapper, Either},
        DateConverter, Marshal, SqlType, StatBuffer, Unmarshal, Value, ValueRef,
    },
};

use super::{column_data::ColumnData, list::List, ColumnFrom};

pub struct DateColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
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
        + convert::Into<Value>
        + convert::From<Value>
        + fmt::Display
        + Sync
        + Default
        + 'static,
{
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        size: usize,
        tz: Tz,
    ) -> Result<DateColumnData<T>, Error> {
        let mut data = List::with_capacity(size);
        data.resize(size, T::default());
        reader.read_bytes(data.as_mut())?;
        Ok(DateColumnData { data, tz })
    }
}

impl ColumnFrom for Vec<DateTime<Tz>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<u32>::with_capacity(source.len());
        for s in source {
            data.push(s.timestamp() as u32);
        }

        let column: DateColumnData<u32> = DateColumnData { data, tz: Tz::Zulu };
        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Date<Tz>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<u16>::with_capacity(source.len());
        for s in source {
            data.push(u16::get_days(s));
        }

        let column: DateColumnData<u16> = DateColumnData { data, tz: Tz::Zulu };
        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Option<DateTime<Tz>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let fake: Vec<DateTime<Tz>> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<BoxColumnWrapper>(fake);

        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };

        for value in source {
            match value {
                None => data.push(Value::Nullable(Either::Left(SqlType::DateTime))),
                Some(d) => {
                    let value = Value::DateTime(d);
                    data.push(Value::Nullable(Either::Right(Box::new(value))))
                }
            }
        }

        W::wrap(data)
    }
}

impl ColumnFrom for Vec<Option<Date<Tz>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let fake: Vec<Date<Tz>> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<BoxColumnWrapper>(fake);

        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };

        for value in source {
            match value {
                None => data.push(Value::Nullable(Either::Left(SqlType::Date))),
                Some(d) => {
                    let value = Value::Date(d);
                    data.push(Value::Nullable(Either::Right(Box::new(value))))
                }
            }
        }

        W::wrap(data)
    }
}

impl<T> ColumnData for DateColumnData<T>
where
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
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
        let start_index = start * mem::size_of::<T>();
        let end_index = end * mem::size_of::<T>();
        let data = self.data.as_ref();
        encoder.write_bytes(&data[start_index..end_index]);
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
}

#[cfg(test)]
mod test {
    use chrono::TimeZone;
    use chrono_tz::Tz;

    use super::*;
    use crate::types::column::ArcColumnWrapper;

    #[test]
    fn test_create_date() {
        let tz = Tz::Zulu;
        let column = Vec::column_from::<ArcColumnWrapper>(vec![tz.ymd(2016, 10, 22)]);
        assert_eq!("2016-10-22UTC", format!("{:#}", column.at(0)));
        assert_eq!(SqlType::Date, column.sql_type());
    }

    #[test]
    fn test_create_date_time() {
        let tz = Tz::Zulu;
        let column =
            Vec::column_from::<ArcColumnWrapper>(vec![tz.ymd(2016, 10, 22).and_hms(12, 0, 0)]);
        assert_eq!("2016-10-22 12:00:00 UTC", format!("{}", column.at(0)));
        assert_eq!(SqlType::DateTime, column.sql_type());
    }
}
