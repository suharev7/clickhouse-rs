use chrono::prelude::*;
use chrono_tz::Tz;

use crate::types::{DateTimeType, SqlType, Value, ValueRef};

pub trait DateConverter {
    fn to_date(&self, tz: Tz) -> ValueRef<'static>;
    fn get_stamp(source: Value) -> Self;
    fn date_type() -> SqlType;

    fn get_days(date: NaiveDate) -> u16 {
        const UNIX_EPOCH_DAY: i64 = 719_163;
        let gregorian_day = i64::from(date.num_days_from_ce());
        (gregorian_day - UNIX_EPOCH_DAY) as u16
    }
}

impl DateConverter for u16 {
    fn to_date(&self, _tz: Tz) -> ValueRef<'static> {
        ValueRef::Date(*self)
    }

    fn get_stamp(source: Value) -> Self {
        Self::get_days(NaiveDate::from(source))
    }

    fn date_type() -> SqlType {
        SqlType::Date
    }
}

impl DateConverter for u32 {
    fn to_date(&self, tz: Tz) -> ValueRef<'static> {
        ValueRef::DateTime(*self, tz)
    }

    fn get_stamp(source: Value) -> Self {
        DateTime::<Tz>::from(source).timestamp() as Self
    }

    fn date_type() -> SqlType {
        SqlType::DateTime(DateTimeType::DateTime32)
    }
}
