use std::{borrow::Cow, marker};

use chrono_tz::Tz;

use crate::{
    errors::{Error, FromSqlError, Result},
    types::{
        block::ColumnIdx,
        column::{ArcColumnWrapper, ColumnData, Either},
        Column, ColumnType, Value,
    },
    Block,
};

pub trait RowBuilder {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()>;
}

pub struct RNil;

pub struct RCons<T>
where
    T: RowBuilder,
{
    key: Cow<'static, str>,
    value: Value,
    tail: T,
}

impl RNil {
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        RCons {
            key,
            value,
            tail: RNil,
        }
    }
}

impl<T> RCons<T>
where
    T: RowBuilder,
{
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        RCons {
            key,
            value,
            tail: self,
        }
    }
}

impl RowBuilder for RNil {
    #[inline(always)]
    fn apply<K: ColumnType>(self, _block: &mut Block<K>) -> Result<()> {
        Ok(())
    }
}

impl<T> RowBuilder for RCons<T>
where
    T: RowBuilder,
{
    #[inline(always)]
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        block.push_value(&self.key, self.value)?;
        self.tail.apply(block)
    }
}

impl RowBuilder for Vec<(String, Value)> {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        for (k, v) in self {
            block.push_value(&k, v)?;
        }
        Ok(())
    }
}

pub fn extract_timezone(value: &Value) -> Tz {
    match value {
        Value::Date(_, tz) => *tz,
        Value::DateTime(_, tz) => *tz,
        Value::Nullable(Either::Right(d)) => extract_timezone(&&d),
        Value::Array(_, data) => {
            if let Some(v) = data.first() {
                extract_timezone(v)
            } else {
                Tz::Zulu
            }
        }
        _ => Tz::Zulu,
    }
}

#[cfg(test)]
mod test {
    use chrono::prelude::*;
    use chrono_tz::Tz::{self, UTC};

    use crate::{
        row,
        types::{DateTimeType, Decimal, Simple, SqlType},
    };

    use super::*;

    #[test]
    fn test_push_row() {
        let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
        let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

        let decimal = Decimal::of(2.0_f64, 4);

        let mut block = Block::<Simple>::new();
        block
            .push(row! {
                i8_field: 1_i8,
                i16_field: 1_i16,
                i32_field: 1_i32,
                i64_field: 1_i64,

                u8_field: 1_u8,
                u16_field: 1_u16,
                u32_field: 1_u32,
                u64_field: 1_u64,

                f32_field: 4.66_f32,
                f64_field: 2.71_f64,

                str_field: "text",
                opt_filed: Some("text"),
                nil_filed: Option::<&str>::None,

                date_field: date_value,
                date_time_field: date_time_value,

                decimal_field: decimal
            })
            .unwrap();

        assert_eq!(block.row_count(), 1);

        assert_eq!(block.columns[0].sql_type(), SqlType::Int8);
        assert_eq!(block.columns[1].sql_type(), SqlType::Int16);
        assert_eq!(block.columns[2].sql_type(), SqlType::Int32);
        assert_eq!(block.columns[3].sql_type(), SqlType::Int64);

        assert_eq!(block.columns[4].sql_type(), SqlType::UInt8);
        assert_eq!(block.columns[5].sql_type(), SqlType::UInt16);
        assert_eq!(block.columns[6].sql_type(), SqlType::UInt32);
        assert_eq!(block.columns[7].sql_type(), SqlType::UInt64);

        assert_eq!(block.columns[8].sql_type(), SqlType::Float32);
        assert_eq!(block.columns[9].sql_type(), SqlType::Float64);

        assert_eq!(block.columns[10].sql_type(), SqlType::String);
        assert_eq!(
            block.columns[11].sql_type(),
            SqlType::Nullable(SqlType::String.into())
        );
        assert_eq!(
            block.columns[12].sql_type(),
            SqlType::Nullable(SqlType::String.into())
        );

        assert_eq!(block.columns[13].sql_type(), SqlType::Date);
        assert_eq!(
            block.columns[14].sql_type(),
            SqlType::DateTime(DateTimeType::Chrono)
        );
        assert_eq!(block.columns[15].sql_type(), SqlType::Decimal(18, 4));
    }
}
