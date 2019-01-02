use chrono::prelude::*;
use chrono::Date;
use chrono_tz::Tz;

use crate::types::{SqlType, Value, ValueRef};

pub trait DateConverter {
    fn to_date(&self, tz: Tz) -> ValueRef<'static>;
    fn get_stamp(source: Value) -> Self;
    fn date_type() -> SqlType;

    fn get_days(date: Date<Tz>) -> u16 {
        const UNIX_EPOCH_DAY: i64 = 719_163;
        let gregorian_day = i64::from(date.num_days_from_ce());
        (gregorian_day - UNIX_EPOCH_DAY) as u16
    }
}

impl DateConverter for u16 {
    fn to_date(&self, tz: Tz) -> ValueRef<'static> {
        let time = tz.timestamp(i64::from(*self) * 24 * 3600, 0);
        ValueRef::Date(time.date())
    }

    fn get_stamp(source: Value) -> u16 {
        Self::get_days(Date::<Tz>::from(source))
    }

    fn date_type() -> SqlType {
        SqlType::Date
    }
}

impl DateConverter for u32 {
    fn to_date(&self, tz: Tz) -> ValueRef<'static> {
        let time = tz.timestamp(i64::from(*self), 0);
        ValueRef::DateTime(time)
    }

    fn get_stamp(source: Value) -> u32 {
        DateTime::<Tz>::from(source).timestamp() as Self
    }

    fn date_type() -> SqlType {
        SqlType::DateTime
    }
}
