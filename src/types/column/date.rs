use std::{convert, fmt, mem, sync::Arc};

use chrono::Date;
use chrono::prelude::*;
use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Error,
    types::{DateConverter, Marshal, SqlType, StatBuffer, Unmarshal, Value, ValueRef},
};

use super::{BoxColumnData, column_data::ColumnData, ColumnFrom, list::List};

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
        + 'static,
{
    pub fn load<R: ReadEx>(
        reader: &mut R,
        size: usize,
        tz: Tz,
    ) -> Result<DateColumnData<T>, Error> {
        let mut row = vec![0_u8; size * mem::size_of::<T>()];
        reader.read_bytes(row.as_mut())?;
        let data = List::from(row);
        Ok(DateColumnData { data, tz })
    }
}

impl ColumnFrom for Vec<DateTime<Tz>> {
    fn column_from(source: Vec<DateTime<Tz>>) -> BoxColumnData {
        let mut data = List::<u32>::with_capacity(source.len());
        for s in source {
            data.push(s.timestamp() as u32);
        }

        let column: DateColumnData<u32> = DateColumnData { data, tz: Tz::Zulu };
        Arc::new(column)
    }
}

impl ColumnFrom for Vec<Date<Tz>> {
    fn column_from(source: Vec<Date<Tz>>) -> BoxColumnData {
        let mut data = List::<u16>::with_capacity(source.len());
        for s in source {
            data.push(u16::get_days(s));
        }

        let column: DateColumnData<u16> = DateColumnData { data, tz: Tz::Zulu };
        Arc::new(column)
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
        + 'static,
{
    fn sql_type(&self) -> SqlType {
        T::date_type()
    }

    fn save(&self, encoder: &mut Encoder) {
        encoder.write_bytes(self.data.as_ref())
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

    #[test]
    fn test_create_date() {
        let tz = Tz::Zulu;
        let column = Vec::column_from(vec![tz.ymd(2016, 10, 22)]);
        assert_eq!("2016-10-22UTC", format!("{:#}", column.at(0)));
        assert_eq!(SqlType::Date, column.sql_type());
    }

    #[test]
    fn test_create_date_time() {
        let tz = Tz::Zulu;
        let column = Vec::column_from(vec![tz.ymd(2016, 10, 22).and_hms(12, 0, 0)]);
        assert_eq!("2016-10-22 12:00:00 UTC", format!("{}", column.at(0)));
        assert_eq!(SqlType::DateTime, column.sql_type());
    }
}
