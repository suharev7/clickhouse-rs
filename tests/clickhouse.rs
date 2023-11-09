extern crate chrono;
extern crate chrono_tz;
extern crate clickhouse_rs;
extern crate tokio;

#[macro_use]
extern crate pretty_assertions;

use chrono::prelude::*;
use chrono_tz::Tz;
use clickhouse_rs::{
    errors::Error,
    row,
    types::{Complex, Decimal, Enum16, Enum8, FromSql, SqlType, Value},
    Block, Options, Pool,
};
use futures_util::{
    future,
    stream::{StreamExt, TryStreamExt},
};
use std::{
    collections::HashMap,
    env, fmt,
    net::{Ipv4Addr, Ipv6Addr},
    str::{self, FromStr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use uuid::Uuid;
use Tz::{Asia__Istanbul as IST, UTC};

#[cfg(not(feature = "tls"))]
fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9000?compression=lz4&ping_timeout=2s&retry_timeout=3s".into()
    })
}

#[cfg(feature = "tls")]
fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9440?compression=lz4&ping_timeout=2s&retry_timeout=3s&secure=true&skip_verify=true".into()
    })
}

fn collect_values<'b, T: FromSql<'b>>(block: &'b Block<Complex>, column: &str) -> Vec<T> {
    (0..block.row_count())
        .map(|i| block.get(i, column).unwrap())
        .collect()
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_ping() -> Result<(), Error> {
    let pool = Pool::new(database_url());

    let mut c = pool.get_handle().await?;
    c.ping().await?;

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_connection_by_wrong_address() -> Result<(), Error> {
    let pool = Pool::new("tcp://badaddr:9000");
    let ret: Result<(), Error> = async move {
        let mut c = pool.get_handle().await?;
        c.ping().await?;
        Ok(())
    }
    .await;

    ret.unwrap_err();
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_create_table() -> Result<(), Error> {
    let ddl = r"
               CREATE TABLE clickhouse_test_create_table (
               click_id   FixedString(64),
               click_time DateTime
               ) Engine=Memory";

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_create_table")
        .await?;
    c.execute(ddl).await?;

    if let Err(err) = c.execute(ddl).await {
        assert_eq!("Server error", &format!("{err}")[..12]);
    } else {
        panic!("should fail")
    }

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_insert() -> Result<(), Error> {
    let ddl = r"
               CREATE TABLE clickhouse_test_insert (
               int8  Int8,
               int16 Int16,
               int32 Int32,
               int64 Int64,
               uint8  UInt8,
               uint16 UInt16,
               uint32 UInt32,
               uint64 UInt64,
               float32 Float32,
               float64 Float64,
               string  String,
               date    Date,
               datetime DateTime,
               datetime64 DateTime64(3, 'UTC'),
               ipv4 IPv4,
               ipv6 IPv6,
               ipv4str IPv4,
               ipv6str IPv6,
               uuid UUID,
               bln Bool
               ) Engine=Memory";

    let block = Block::new()
        .column("int8", vec![-1_i8, -2, -3, -4, -5, -6, -7])
        .column("int16", vec![-1_i16, -2, -3, -4, -5, -6, -7])
        .column("int32", vec![-1_i32, -2, -3, -4, -5, -6, -7])
        .column("int64", vec![-1_i64, -2, -3, -4, -5, -6, -7])
        .column("uint8", vec![1_u8, 2, 3, 4, 5, 6, 7])
        .column("uint16", vec![1_u16, 2, 3, 4, 5, 6, 7])
        .column("uint32", vec![1_u32, 2, 3, 4, 5, 6, 7])
        .column("uint64", vec![1_u64, 2, 3, 4, 5, 6, 7])
        .column("float32", vec![1.0_f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        .column("float64", vec![1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        .column("string", vec!["1", "2", "3", "4", "5", "6", "7"])
        .column(
            "date",
            vec![
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
            ],
        )
        .column(
            "datetime",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            ],
        )
        .column(
            "datetime64",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            ],
        )
        .column(
            "ipv4",
            vec![
                Ipv4Addr::new(10, 10, 10, 10),
                Ipv4Addr::new(172, 16, 10, 10),
                Ipv4Addr::new(172, 29, 45, 14),
                Ipv4Addr::new(10, 10, 10, 10),
                Ipv4Addr::new(172, 16, 10, 10),
                Ipv4Addr::new(172, 29, 45, 14),
                Ipv4Addr::new(10, 10, 10, 10),
            ],
        )
        .column(
            "ipv6",
            vec![
                Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
                Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff),
                Ipv6Addr::new(0, 0, 0x1c9, 0, 0, 0xafc8, 0, 0x1),
                Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
                Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff),
                Ipv6Addr::new(0, 0, 0x1c9, 0, 0, 0xafc8, 0, 0x1),
                Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
            ],
        )
        .column(
            "ipv4str",
            vec![
                "10.10.10.10",
                "172.16.10.10",
                "172.29.45.14",
                "10.10.10.10",
                "172.16.10.10",
                "172.29.45.14",
                "10.10.10.10",
            ],
        )
        .column(
            "ipv6str",
            vec![
                "2a02:aa08:e000:3100::2",
                "2001:44c8:129:2632:33:0:252:2",
                "2a02:e980:1e::1",
                "2a02:aa08:e000:3100::2",
                "2001:44c8:129:2632:33:0:252:2",
                "2a02:e980:1e::1",
                "2a02:aa08:e000:3100::2",
            ],
        )
        .column(
            "uuid",
            vec![
                Uuid::parse_str("936DA01F-9ABD-4d9d-80C7-02AF85C822A8").unwrap(),
                Uuid::nil(),
                Uuid::nil(),
                Uuid::nil(),
                Uuid::nil(),
                Uuid::nil(),
                Uuid::nil(),
            ],
        )
        .column("bln", vec![true, false, true, false, true, false, true]);

    let expected = block.clone();
    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_insert")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_insert", block).await?;
    let actual = c
        .query("SELECT * FROM clickhouse_test_insert")
        .fetch_all()
        .await?;

    assert_eq!(format!("{:?}", expected.as_ref()), format!("{:?}", &actual));
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_datetime_read_write() -> Result<(), Error> {
    let db = "clickhouse_test_datetime_read_write";

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;

    c.execute(format!("DROP TABLE IF EXISTS {db}")).await?;
    c.execute(format!(
        "CREATE TABLE {db} (
            datetime DateTime,
            datetime_utc DateTime('UTC'),
            datetime_ist DateTime('Asia/Istanbul'),
            datetime_opt Nullable(DateTime),
            datetime_opt_utc Nullable(DateTime('UTC')),
            datetime_opt_ist Nullable(DateTime('Asia/Istanbul'))
        ) Engine=Memory"
    ))
    .await?;

    let block = Block::new()
        .column(
            "datetime",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap(),
            ],
        )
        .column(
            "datetime_utc",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap(),
            ],
        )
        .column(
            "datetime_ist",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap(),
            ],
        )
        .column(
            "datetime_opt",
            vec![
                Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
                Some(IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap()),
                None,
            ],
        )
        .column(
            "datetime_opt_utc",
            vec![
                Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
                Some(IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap()),
                None,
            ],
        )
        .column(
            "datetime_opt_ist",
            vec![
                Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
                Some(IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0).unwrap()),
                None,
            ],
        );

    c.insert(&db, block).await?;

    let actual_strings_block = c
        .query(format!(
            "SELECT
                toString(datetime) as datetime_s,
                toString(datetime_utc) as datetime_utc_s,
                toString(datetime_ist) as datetime_ist_s,
                toString(datetime_opt) as datetime_opt_s,
                toString(datetime_opt_utc) as datetime_opt_utc_s,
                toString(datetime_opt_ist) as datetime_opt_ist_s
            FROM {db}",
        ))
        .fetch_all()
        .await?;

    let expected_strings_block = Block::new()
        .column(
            "datetime_s",
            vec![
                "2016-10-22 12:00:00".to_string(),
                "2016-10-22 12:00:00".to_string(),
                "2016-10-22 12:00:00".to_string(),
            ],
        )
        .column(
            "datetime_utc_s",
            vec![
                "2016-10-22 12:00:00".to_string(),
                "2016-10-22 12:00:00".to_string(),
                "2016-10-22 12:00:00".to_string(),
            ],
        )
        .column(
            "datetime_ist_s",
            vec![
                "2016-10-22 15:00:00".to_string(),
                "2016-10-22 15:00:00".to_string(),
                "2016-10-22 15:00:00".to_string(),
            ],
        )
        .column(
            "datetime_opt_s",
            vec![
                "2016-10-22 12:00:00".to_string(),
                "2016-10-22 12:00:00".to_string(),
                "NULL".to_string(),
            ],
        )
        .column(
            "datetime_opt_utc_s",
            vec![
                "2016-10-22 12:00:00".to_string(),
                "2016-10-22 12:00:00".to_string(),
                "NULL".to_string(),
            ],
        )
        .column(
            "datetime_opt_ist_s",
            vec![
                "2016-10-22 15:00:00".to_string(),
                "2016-10-22 15:00:00".to_string(),
                "NULL".to_string(),
            ],
        );

    assert_eq!(
        format!("{expected_strings_block:?}"),
        format!("{actual_strings_block:?}")
    );

    let actual_dates_block = c
        .query(format!(
            "SELECT
                datetime,
                datetime_utc,
                datetime_ist,
                datetime_opt,
                datetime_opt_utc,
                datetime_opt_ist
            FROM {db}"
        ))
        .fetch_all()
        .await?;

    assert_eq!(
        vec![
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
        ],
        collect_values(&actual_dates_block, "datetime")
    );

    assert_eq!(
        vec![
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
        ],
        collect_values(&actual_dates_block, "datetime_utc")
    );

    assert_eq!(
        vec![
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
        ],
        collect_values(&actual_dates_block, "datetime_ist")
    );

    assert_eq!(
        vec![
            Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
            Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
            None,
        ],
        collect_values(&actual_dates_block, "datetime_opt")
    );

    assert_eq!(
        vec![
            Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
            Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
            None,
        ],
        collect_values(&actual_dates_block, "datetime_opt_utc")
    );

    assert_eq!(
        vec![
            Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
            Some(UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()),
            None,
        ],
        collect_values(&actual_dates_block, "datetime_opt_ist")
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_datetime64_read_write() -> Result<(), Error> {
    let db = "clickhouse_test_datetime64_read_write";

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;

    c.execute(format!("DROP TABLE IF EXISTS {db}")).await?;
    c.execute(format!(
        "CREATE TABLE {db} (
            datetime64 DateTime64(3),
            datetime64_utc DateTime(3, 'UTC'),
            datetime64_ist DateTime(3, 'Asia/Istanbul'),
            datetime64_opt Nullable(DateTime64(3)),
            datetime64_opt_utc Nullable(DateTime(3, 'UTC')),
            datetime64_opt_ist Nullable(DateTime(3, 'Asia/Istanbul'))
        ) Engine=Memory"
    ))
    .await?;

    let block = Block::new()
        .column(
            "datetime64",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
            ],
        )
        .column(
            "datetime64_utc",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
            ],
        )
        .column(
            "datetime64_ist",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
                IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_456_678)
                    .unwrap(),
            ],
        )
        .column(
            "datetime64_opt",
            vec![
                Some(
                    UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                        .unwrap()
                        .with_nanosecond(123_456_678)
                        .unwrap(),
                ),
                Some(
                    IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                        .unwrap()
                        .with_nanosecond(123_456_678)
                        .unwrap(),
                ),
                None,
            ],
        )
        .column(
            "datetime64_opt_utc",
            vec![
                Some(
                    UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                        .unwrap()
                        .with_nanosecond(123_456_678)
                        .unwrap(),
                ),
                Some(
                    IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                        .unwrap()
                        .with_nanosecond(123_456_678)
                        .unwrap(),
                ),
                None,
            ],
        )
        .column(
            "datetime64_opt_ist",
            vec![
                Some(
                    UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                        .unwrap()
                        .with_nanosecond(123_456_678)
                        .unwrap(),
                ),
                Some(
                    IST.with_ymd_and_hms(2016, 10, 22, 15, 0, 0)
                        .unwrap()
                        .with_nanosecond(123_456_678)
                        .unwrap(),
                ),
                None,
            ],
        );

    c.insert(&db, block).await?;

    let actual_strings_block = c
        .query(format!(
            "SELECT
                toString(datetime64) as datetime64_s,
                toString(datetime64_utc) as datetime64_utc_s,
                toString(datetime64_ist) as datetime64_ist_s,
                toString(datetime64_opt) as datetime64_opt_s,
                toString(datetime64_opt_utc) as datetime64_opt_utc_s,
                toString(datetime64_opt_ist) as datetime64_opt_ist_s
            FROM {db}"
        ))
        .fetch_all()
        .await?;

    let expected_strings_block = Block::new()
        .column(
            "datetime64_s",
            vec![
                "2016-10-22 12:00:00.123".to_string(),
                "2016-10-22 12:00:00.123".to_string(),
                "2016-10-22 12:00:00.123".to_string(),
            ],
        )
        .column(
            "datetime64_utc_s",
            vec![
                "2016-10-22 12:00:00.123".to_string(),
                "2016-10-22 12:00:00.123".to_string(),
                "2016-10-22 12:00:00.123".to_string(),
            ],
        )
        .column(
            "datetime64_ist_s",
            vec![
                "2016-10-22 15:00:00.123".to_string(),
                "2016-10-22 15:00:00.123".to_string(),
                "2016-10-22 15:00:00.123".to_string(),
            ],
        )
        .column(
            "datetime64_opt_s",
            vec![
                "2016-10-22 12:00:00.123".to_string(),
                "2016-10-22 12:00:00.123".to_string(),
                "NULL".to_string(),
            ],
        )
        .column(
            "datetime64_opt_utc_s",
            vec![
                "2016-10-22 12:00:00.123".to_string(),
                "2016-10-22 12:00:00.123".to_string(),
                "NULL".to_string(),
            ],
        )
        .column(
            "datetime64_opt_ist_s",
            vec![
                "2016-10-22 15:00:00.123".to_string(),
                "2016-10-22 15:00:00.123".to_string(),
                "NULL".to_string(),
            ],
        );

    assert_eq!(
        format!("{expected_strings_block:?}"),
        format!("{actual_strings_block:?}")
    );

    let actual_dates_block = c
        .query(format!(
            "SELECT
                datetime64,
                datetime64_utc,
                datetime64_ist,
                datetime64_opt,
                datetime64_opt_utc,
                datetime64_opt_ist
            FROM {db}"
        ))
        .fetch_all()
        .await?;

    assert_eq!(
        vec![
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
        ],
        collect_values(&actual_dates_block, "datetime64")
    );

    assert_eq!(
        vec![
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
        ],
        collect_values(&actual_dates_block, "datetime64_utc")
    );

    assert_eq!(
        vec![
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                .unwrap()
                .with_nanosecond(123_000_000)
                .unwrap(),
        ],
        collect_values(&actual_dates_block, "datetime64_ist")
    );

    assert_eq!(
        vec![
            Some(
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_000_000)
                    .unwrap(),
            ),
            Some(
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_000_000)
                    .unwrap(),
            ),
            None,
        ],
        collect_values(&actual_dates_block, "datetime64_opt")
    );

    assert_eq!(
        vec![
            Some(
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_000_000)
                    .unwrap(),
            ),
            Some(
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_000_000)
                    .unwrap(),
            ),
            None,
        ],
        collect_values(&actual_dates_block, "datetime64_opt_utc")
    );

    assert_eq!(
        vec![
            Some(
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_000_000)
                    .unwrap(),
            ),
            Some(
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0)
                    .unwrap()
                    .with_nanosecond(123_000_000)
                    .unwrap(),
            ),
            None,
        ],
        collect_values(&actual_dates_block, "datetime64_opt_ist")
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_empty_select() -> Result<(), Error> {
    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;

    let r = c.query("SELECT 1 WHERE 1 <> 1").fetch_all().await?;

    assert_eq!(r.row_count(), 0);
    assert_eq!(r.column_count(), 1);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_select_settings() -> Result<(), Error> {
    let options = Options::from_str(&database_url())?.with_setting(
        "max_threads",
        1,
        /* is_important= */ true,
    );
    let pool = Pool::new(options);
    let mut c = pool.get_handle().await?;

    let r = c.query("SELECT 1 WHERE 1 <> 1").fetch_all().await?;

    assert_eq!(r.row_count(), 0);
    assert_eq!(r.column_count(), 1);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
#[should_panic]
async fn test_select_unknown_settings() {
    let options = Options::from_str(&database_url())
        .unwrap()
        .with_setting("foo", 1, /* is_important= */ true);
    let pool = Pool::new(options);
    let mut c = pool.get_handle().await.unwrap();
    c.query("SELECT 1 WHERE 1 <> 1").fetch_all().await.unwrap();
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_select() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_test_select (
            id       Int32,
            code     String,
            date     Date,
            datetime DateTime
        ) Engine=Memory";

    let block = Block::new()
        .column("id", vec![1, 2, 3, 4])
        .column("code", vec!["RU", "UA", "DE", "US"])
        .column(
            "date",
            vec![
                NaiveDate::from_ymd_opt(2014, 7, 8).unwrap(),
                NaiveDate::from_ymd_opt(2014, 7, 8).unwrap(),
                NaiveDate::from_ymd_opt(2014, 7, 8).unwrap(),
                NaiveDate::from_ymd_opt(2014, 7, 9).unwrap(),
            ],
        )
        .column(
            "datetime",
            vec![
                Tz::Singapore
                    .with_ymd_and_hms(2014, 7, 8, 14, 0, 0)
                    .unwrap(),
                UTC.with_ymd_and_hms(2014, 7, 8, 14, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2014, 7, 8, 14, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2014, 7, 8, 13, 0, 0).unwrap(),
            ],
        );

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_select")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_select", block).await?;

    let r = c
        .query("SELECT COUNT(*) FROM clickhouse_test_select")
        .fetch_all()
        .await?;
    assert_eq!(4, r.get::<u64, _>(0, 0)?);

    let r = c
        .query("SELECT COUNT(*) FROM clickhouse_test_select WHERE date = '2014-07-08'")
        .fetch_all()
        .await?;
    assert_eq!(3, r.get::<u64, _>(0, 0)?);

    let r = c
        .query("SELECT COUNT(*) FROM clickhouse_test_select WHERE datetime = '2014-07-08 14:00:00'")
        .fetch_all()
        .await?;
    assert_eq!(2, r.get::<u64, _>(0, 0)?);

    let r = c
        .query("SELECT COUNT(*) FROM clickhouse_test_select WHERE id IN (1, 2, 3)")
        .fetch_all()
        .await?;
    assert_eq!(3, r.get::<u64, _>(0, 0)?);

    let r = c
        .query("SELECT COUNT(*) FROM clickhouse_test_select WHERE code IN ('US', 'DE', 'RU')")
        .fetch_all()
        .await?;
    assert_eq!(3, r.get::<u64, _>(0, 0)?);

    let r = c
        .query("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT 1")
        .fetch_all()
        .await?;
    assert_eq!(r.row_count(), 1);
    assert_eq!(1, r.get::<i32, _>(0, "id")?);

    let r = c
        .query("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT 1, 2")
        .fetch_all()
        .await?;
    assert_eq!(r.row_count(), 2);
    assert_eq!(2, r.get::<i32, _>(0, "id")?);
    assert_eq!(3, r.get::<i32, _>(1, 0)?);

    Ok(())
}

#[cfg(feature = "async_std")]
#[test]
fn test_simple_select() {
    use async_std::task;

    async fn execute() -> Result<(), Error> {
        let pool = Pool::new(database_url());
        let mut c = pool.get_handle().await?;
        let actual = c.query("SELECT 1 as A").fetch_all().await?;

        let expected = Block::new().column("A", vec![1_u8]);
        assert_eq!(expected, actual);

        Ok(())
    }

    task::block_on(execute()).unwrap();
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_simple_selects() -> Result<(), Error> {
    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    let actual = c.query("SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a) ORDER BY a ASC").fetch_all().await?;

    let expected = Block::new().column("a", vec![1_u8, 2, 3]);
    assert_eq!(expected, actual);

    let r = c
        .query("SELECT min(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)")
        .fetch_all()
        .await?;
    assert_eq!(1, r.get::<u8, _>(0, 0)?);

    let r = c
        .query("SELECT max(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)")
        .fetch_all()
        .await?;
    assert_eq!(3, r.get::<u8, _>(0, 0)?);

    let r = c
        .query("SELECT sum(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)")
        .fetch_all()
        .await?;
    assert_eq!(6, r.get::<u64, _>(0, 0)?);

    let r = c
        .query(
            "SELECT median(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)",
        )
        .fetch_all()
        .await?;
    assert!((2_f64 - r.get::<f64, _>(0, 0)?).abs() < f64::EPSILON);

    let expected = Block::new().column("a", vec![1_u8, 2, 3]);
    assert_eq!(expected, actual);

    let r = c
        .query("SELECT min(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)")
        .fetch_all()
        .await?;
    assert_eq!(1, r.get::<u8, _>(0, 0)?);

    let r = c
        .query("SELECT max(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)")
        .fetch_all()
        .await?;
    assert_eq!(3, r.get::<u8, _>(0, 0)?);

    let r = c
        .query("SELECT sum(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)")
        .fetch_all()
        .await?;
    assert_eq!(6, r.get::<u64, _>(0, 0)?);

    let r = c
        .query(
            "SELECT median(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)",
        )
        .fetch_all()
        .await?;
    assert!((2_f64 - r.get::<f64, _>(0, 0)?).abs() < f64::EPSILON);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_temporary_table() -> Result<(), Error> {
    let ddl = "CREATE TEMPORARY TABLE clickhouse_test_temporary_table (ID UInt64);";

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute(ddl).await?;
    c.execute(
        "INSERT INTO clickhouse_test_temporary_table (ID) \
         SELECT number AS ID FROM system.numbers LIMIT 10",
    )
    .await?;
    let block = c
        .query("SELECT ID AS ID FROM clickhouse_test_temporary_table")
        .fetch_all()
        .await?;

    let expected = Block::new().column("ID", (0_u64..10).collect::<Vec<_>>());
    assert_eq!(block, expected);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_with_totals() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_test_with_totals (
            country String
        ) Engine=Memory";

    let query = "
        SELECT
            country,
            COUNT(*)
        FROM clickhouse_test_with_totals
        GROUP BY country
            WITH TOTALS";

    let block = Block::new().column("country", vec!["RU", "EN", "RU", "RU", "EN", "RU"]);

    let expected = Block::new()
        .column("country", vec!["EN", "RU", ""])
        .column("country", vec![2_u64, 4, 6]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_with_totals")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_with_totals", block).await?;
    let block = c.query(query).fetch_all().await?;

    assert_eq!(expected, block);
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_stream_rows() -> Result<(), Error> {
    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    let actual = c
        .query("SELECT number FROM system.numbers LIMIT 10")
        .stream()
        .try_fold(0_u64, |acc, row| {
            let number: u64 = row.get("number").unwrap();
            future::ready(Ok(acc + number))
        })
        .await?;
    c.ping().await?;

    assert_eq!(45, actual);
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_concurrent_queries() -> Result<(), Error> {
    async fn query_sum(n: u64) -> Result<u64, Error> {
        let sql = format!("SELECT number FROM system.numbers LIMIT {n}");

        let pool = Pool::new(database_url());
        let mut c = pool.get_handle().await?;
        let block = c.query(sql.as_str()).fetch_all().await?;

        let mut total = 0_u64;
        for row in 0_usize..block.row_count() {
            let x: u64 = block.get(row, "number")?;
            total += x;
        }
        Ok(total)
    }

    let m = 250_000_u64;

    let expected = m * (m - 1) / 2
        + (m * 2) * ((m * 2) - 1) / 2
        + (m * 3) * ((m * 3) - 1) / 2
        + (m * 4) * ((m * 4) - 1) / 2;

    let requests = vec![
        query_sum(m),
        query_sum(m * 2),
        query_sum(m * 3),
        query_sum(m * 4),
    ];

    let xs = future::join_all(requests).await;
    let mut actual: u64 = 0;

    for x in xs {
        actual += x?;
    }

    assert_eq!(actual, expected);
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_big_block() -> Result<(), Error> {
    let sql = "SELECT
        number, number, number, number, number, number, number, number, number, number
        FROM system.numbers LIMIT 20000";

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    let block = c.query(sql).fetch_all().await?;

    assert_eq!(block.row_count(), 20000);
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_nullable() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_test_nullable (
            int8       Nullable(Int8),
            int16      Nullable(Int16),
            int32      Nullable(Int32),
            int64      Nullable(Int64),
            uint8      Nullable(UInt8),
            uint16     Nullable(UInt16),
            uint32     Nullable(UInt32),
            uint64     Nullable(UInt64),
            float32    Nullable(Float32),
            float64    Nullable(Float64),
            string     Nullable(String),
            date       Nullable(Date),
            datetime   Nullable(DateTime),
            datetime64 Nullable(DateTime64(3, 'UTC')),
            ipv4       Nullable(IPv4),
            ipv6       Nullable(IPv6),
            uuid       Nullable(UUID),
            bln        Nullable(Bool)
        ) Engine=Memory";

    let query = "
        SELECT
            int8,
            int16,
            int32,
            int64,
            uint8,
            uint16,
            uint32,
            uint64,
            float32,
            float64,
            string,
            date,
            datetime,
            datetime64,
            ipv4,
            ipv6,
            uuid,
            bln
        FROM clickhouse_test_nullable";

    let date_value: NaiveDate = NaiveDate::from_ymd_opt(2016, 10, 22).unwrap();
    let date_time_value: DateTime<Tz> = UTC.with_ymd_and_hms(2014, 7, 8, 14, 0, 0).unwrap();

    let block = Block::new()
        .column("int8", vec![Some(1_i8)])
        .column("int16", vec![Some(1_i16)])
        .column("int32", vec![Some(1_i32)])
        .column("int64", vec![Some(1_i64)])
        .column("uint8", vec![Some(1_u8)])
        .column("uint16", vec![Some(1_u16)])
        .column("uint32", vec![Some(1_u32)])
        .column("uint64", vec![Some(1_u64)])
        .column("float32", vec![Some(1_f32)])
        .column("float64", vec![Some(1_f64)])
        .column("string", vec![Some("text")])
        .column("date", vec![Some(date_value)])
        .column("datetime", vec![Some(date_time_value)])
        .column("datetime64", vec![Some(date_time_value)])
        .column("ipv4", vec![Some(Ipv4Addr::new(127, 0, 0, 1))])
        .column(
            "ipv6",
            vec![Some(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff))],
        )
        .column(
            "uuid",
            vec![Some(
                Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap(),
            )],
        )
        .column("bln", vec![Some(true)]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_nullable")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_nullable", block).await?;
    let block = c.query(query).fetch_all().await?;

    let int8: Option<i8> = block.get(0, "int8")?;
    let int16: Option<i16> = block.get(0, "int16")?;
    let int32: Option<i32> = block.get(0, "int32")?;
    let int64: Option<i64> = block.get(0, "int64")?;
    let uint8: Option<u8> = block.get(0, "uint8")?;
    let uint16: Option<u16> = block.get(0, "uint16")?;
    let uint32: Option<u32> = block.get(0, "uint32")?;
    let uint64: Option<u64> = block.get(0, "uint64")?;
    let float32: Option<f32> = block.get(0, "float32")?;
    let float64: Option<f64> = block.get(0, "float64")?;
    let string: Option<&str> = block.get(0, "string")?;
    let date: Option<NaiveDate> = block.get(0, "date")?;
    let datetime: Option<DateTime<Tz>> = block.get(0, "datetime")?;
    let datetime64: Option<DateTime<Tz>> = block.get(0, "datetime64")?;
    let ipv4: Option<Ipv4Addr> = block.get(0, "ipv4")?;
    let ipv6: Option<Ipv6Addr> = block.get(0, "ipv6")?;
    let uuid: Option<Uuid> = block.get(0, "uuid")?;
    let bln: Option<bool> = block.get(0, "bln")?;

    assert_eq!(int8, Some(1_i8));
    assert_eq!(int16, Some(1_i16));
    assert_eq!(int32, Some(1_i32));
    assert_eq!(int64, Some(1_i64));
    assert_eq!(uint8, Some(1_u8));
    assert_eq!(uint16, Some(1_u16));
    assert_eq!(uint32, Some(1_u32));
    assert_eq!(uint64, Some(1_u64));
    assert_eq!(float32, Some(1_f32));
    assert_eq!(float64, Some(1_f64));
    assert_eq!(string, Some("text"));
    assert_eq!(date, Some(date_value));
    assert_eq!(datetime, Some(date_time_value));
    assert_eq!(datetime64, Some(date_time_value));

    assert_eq!(ipv4, Some(Ipv4Addr::new(127, 0, 0, 1)));
    assert_eq!(
        ipv6,
        Some(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff))
    );

    assert_eq!(
        uuid,
        Some(Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap())
    );
    assert_eq!(bln, Some(true),);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[test]
fn test_generic_column() {
    fn extract_to_vec<'a, T>(name: &str, block: &'a Block) -> Vec<T>
    where
        T: FromSql<'a> + fmt::Debug + 'static,
    {
        let n = block.row_count();
        let mut result = Vec::with_capacity(n);
        for row_index in 0..n {
            let value: T = block.get(row_index, name).unwrap();
            result.push(value)
        }
        result
    }

    let block = Block::new()
        .column("int", vec![1u32, 2, 3])
        .column("str", vec!["A", "B", "C"]);

    let int_vec: Vec<u32> = extract_to_vec("int", &block);
    let str_vec: Vec<String> = extract_to_vec("str", &block);

    assert_eq!(int_vec, vec![1u32, 2, 3]);
    assert_eq!(
        str_vec,
        vec!["A".to_string(), "B".to_string(), "C".to_string()]
    );
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_fixed_string() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_test_fixed_string (
            text     FixedString(4),
            opt_text Nullable(FixedString(4))
        ) Engine=Memory";

    let query = "
        SELECT
            text,
            opt_text
        FROM clickhouse_test_fixed_string";

    let block = Block::new()
        .column("opt_text", vec![Some("text")])
        .column("text", vec!["text"]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_fixed_string")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_fixed_string", block).await?;
    let block = c.query(query).fetch_all().await?;

    let text: &str = block.get(0, "text")?;
    let opt_text: Option<&str> = block.get(0, "opt_text")?;

    assert_eq!(text, "text");
    assert_eq!(opt_text, Some("text"));

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_foreign_columns() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_foreign_columns (
            column1      UInt32,
            `中文key`     UInt32,
            `русскийkey` UInt32
        ) Engine=Memory";

    let query = "
        SELECT
            column1,
            `中文key`,
            `русскийkey`
        FROM clickhouse_foreign_columns";

    let block = Block::new()
        .column("column1", vec![1u32, 10u32])
        .column("中文key", vec![2u32, 10u32])
        .column("русскийkey", vec![3u32, 10u32]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_foreign_columns")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_foreign_columns", block).await?;
    let block = c.query(query).fetch_all().await?;

    let v1: u32 = block.get(0, "column1")?;
    let v2: u32 = block.get(0, "中文key")?;
    let v3: u32 = block.get(0, "русскийkey")?;

    assert_eq!(v1, 1u32);
    assert_eq!(v2, 2u32);
    assert_eq!(v3, 3u32);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_binary_string() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_binary_string (
            text        String,
            fx_text     FixedString(4),
            opt_text    Nullable(String),
            fx_opt_text Nullable(FixedString(4))
        ) Engine=Memory";

    let query = "
        SELECT
            text,
            fx_text,
            opt_text,
            fx_opt_text
        FROM clickhouse_binary_string";

    let block = Block::new()
        .column("text", vec![vec![0_u8, 159, 146, 150]])
        .column("fx_text", vec![vec![0_u8, 159, 146, 150]])
        .column("opt_text", vec![Some(vec![0_u8, 159, 146, 150])])
        .column("fx_opt_text", vec![Some(vec![0_u8, 159, 146, 150])]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_binary_string")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_binary_string", block).await?;
    let block = c.query(query).fetch_all().await?;

    let text: &[u8] = block.get(0, "text")?;
    let fx_text: &[u8] = block.get(0, "fx_text")?;
    let opt_text: Option<&[u8]> = block.get(0, "opt_text")?;
    let fx_opt_text: Option<&[u8]> = block.get(0, "fx_opt_text")?;

    assert_eq!(1, block.row_count());
    assert_eq!([0, 159, 146, 150].as_ref(), text);
    assert_eq!([0, 159, 146, 150].as_ref(), fx_text);
    assert_eq!(Some([0, 159, 146, 150].as_ref()), opt_text);
    assert_eq!(Some([0, 159, 146, 150].as_ref()), fx_opt_text);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_enum_16_not_nullable() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_enum16_non_nul (
            enum_16_row Enum16(
                          'zero' = 5,
                          'first' = 6
                        )
        ) Engine=Memory";

    let query = "
        SELECT
            enum_16_row
        FROM clickhouse_enum16_non_nul";

    let block = Block::new().column("enum_16_row", vec![Enum16::of(5), Enum16::of(6)]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_enum16_non_nul")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_enum16_non_nul", block).await?;
    let block = c.query(query).fetch_all().await?;

    let enum_16_a: Enum16 = block.get(0, "enum_16_row")?;
    let enum_16_b: Enum16 = block.get(1, "enum_16_row")?;

    assert_eq!(2, block.row_count());
    assert_eq!(
        vec!([Enum16::of(5), Enum16::of(6)]),
        vec!([enum_16_a, enum_16_b])
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_enum_16_nullable() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_enum (
            enum_16_row        Nullable(Enum16(
                                'zero' = 5,
                                'first' = 6
                          ))
        ) Engine=Memory";

    let query = "
        SELECT
            enum_16_row
        FROM clickhouse_enum";

    let block = Block::new().column(
        "enum_16_row",
        vec![
            Some(Enum16::of(5)),
            Some(Enum16::of(6)),
            Option::<Enum16>::None,
        ],
    );

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_enum").await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_enum", block).await?;
    let block = c.query(query).fetch_all().await?;

    let enum_16_a: Option<Enum16> = block.get(0, "enum_16_row")?;
    let enum_16_b: Option<Enum16> = block.get(1, "enum_16_row")?;

    assert_eq!(3, block.row_count());
    assert_eq!(
        vec!([Some(Enum16::of(5)), Some(Enum16::of(6))]),
        vec!([enum_16_a, enum_16_b])
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_enum_16_array() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_enum_arr (
            enum_16_arr_row Array(Enum16(
                               'zero' = 5,
                               'first' = 6
                            ))
        ) Engine=Memory";

    let query = "
        SELECT
            enum_16_arr_row
        FROM clickhouse_enum_arr";

    let block = Block::new().column(
        "enum_16_arr_row",
        vec![vec![Enum16::of(5)], vec![Enum16::of(6)]],
    );

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_enum_arr")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_enum_arr", block).await?;
    let block = c.query(query).fetch_all().await?;

    let enum_16_a: Vec<Enum16> = block.get(0, "enum_16_arr_row")?;
    let enum_16_b: Vec<Enum16> = block.get(1, "enum_16_arr_row")?;

    assert_eq!(2, block.row_count());
    assert_eq!(
        vec![vec![Enum16::of(5)], vec![Enum16::of(6)]],
        vec![enum_16_a, enum_16_b]
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_enum_8() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_Enum (
            enum_8_row  Enum8(
                            'zero' = 1,
                            'first' = 2
                        )
        ) Engine=Memory";

    let query = "
        SELECT
            enum_8_row
        FROM clickhouse_Enum";

    let block = Block::new().column("enum_8_row", vec![Enum8::of(1), Enum8::of(2)]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_Enum").await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_Enum", block).await?;
    let block = c.query(query).fetch_all().await?;

    let enum_8_a: Enum8 = block.get(0, "enum_8_row")?;
    let enum_8_b: Enum8 = block.get(1, "enum_8_row")?;

    assert_eq!(2, block.row_count());
    assert_eq!(
        vec!([Enum8::of(1), Enum8::of(2)]),
        vec!([enum_8_a, enum_8_b])
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_enum_8_array() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_enum8_arr (
            enum_8_arr_row Array(Enum8(
                               'zero' = 5,
                               'first' = 6
                            ))
        ) Engine=Memory";

    let query = "
        SELECT
            enum_8_arr_row
        FROM clickhouse_enum8_arr";

    let block = Block::new().column(
        "enum_8_arr_row",
        vec![vec![Enum8::of(5)], vec![Enum8::of(6)]],
    );

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_enum8_arr")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_enum8_arr", block).await?;
    let block = c.query(query).fetch_all().await?;

    let enum_16_a: Vec<Enum8> = block.get(0, "enum_8_arr_row")?;
    let enum_16_b: Vec<Enum8> = block.get(1, "enum_8_arr_row")?;

    assert_eq!(2, block.row_count());
    assert_eq!(
        vec![vec![Enum8::of(5)], vec![Enum8::of(6)]],
        vec![enum_16_a, enum_16_b]
    );

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_array() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_array (
            u8      Array(UInt8),
            u32     Array(UInt32),
            f64     Array(Float64),
            text1   Array(String),
            text2   Array(String),
            date    Array(Date),
            time    Array(DateTime),
            time64  Array(DateTime64(3, 'UTC')),
            bln     Array(Bool)
        ) Engine=Memory";

    let query = "SELECT u8, u32, f64, text1, text2, date, time, time64, bln  FROM clickhouse_array";

    let date_value: NaiveDate = NaiveDate::from_ymd_opt(2016, 10, 22).unwrap();
    let date_time_value: DateTime<Tz> = UTC.with_ymd_and_hms(2014, 7, 8, 14, 0, 0).unwrap();

    let block = Block::new()
        .column("u8", vec![vec![41_u8]])
        .column("u32", vec![vec![42_u32]])
        .column("f64", vec![vec![42_f64]])
        .column("text1", vec![vec!["A"]])
        .column("text2", vec![vec!["B".to_string()]])
        .column("date", vec![vec![date_value]])
        .column("time", vec![vec![date_time_value]])
        .column("time64", vec![vec![date_time_value]])
        .column("bln", vec![vec![true]]);

    let pool = Pool::new(database_url());

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_array").await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_array", block).await?;
    let block = c.query(query).fetch_all().await?;

    let u8_vec: Vec<u8> = block.get(0, "u8")?;
    let u32_vec: Vec<u32> = block.get(0, "u32")?;
    let f64_vec: Vec<f64> = block.get(0, "f64")?;
    let text1_vec: Vec<&str> = block.get(0, "text1")?;
    let text2_vec: Vec<String> = block.get(0, "text2")?;
    let date_vec: Vec<NaiveDate> = block.get(0, "date")?;
    let time_vec: Vec<DateTime<Tz>> = block.get(0, "time")?;
    let time64_vec: Vec<DateTime<Tz>> = block.get(0, "time64")?;
    let bln_vec: Vec<bool> = block.get(0, "bln")?;

    assert_eq!(1, block.row_count());
    assert_eq!(vec![41_u8], u8_vec);
    assert_eq!(vec![42_u32], u32_vec);
    assert_eq!(vec![42_f64], f64_vec);
    assert_eq!(vec!["A"], text1_vec);
    assert_eq!(vec!["B".to_string()], text2_vec);
    assert_eq!(vec![date_value], date_vec);
    assert_eq!(vec![date_time_value], time_vec);
    assert_eq!(vec![date_time_value], time64_vec);
    assert_eq!(vec![true], bln_vec);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[allow(clippy::float_cmp)]
#[tokio::test]
async fn test_decimal() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_decimal (
            x  Decimal(8, 3),
            ox Nullable(Decimal(10, 2))
        ) Engine=Memory";

    let query = "SELECT x, ox FROM clickhouse_decimal";

    let block = Block::new()
        .column("x", vec![Decimal::of(1.234, 3), Decimal::of(5, 3)])
        .column("ox", vec![None, Some(Decimal::of(1.23, 2))]);

    let pool = Pool::new(database_url());

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_decimal").await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_decimal", block).await?;
    let block = c.query(query).fetch_all().await?;

    let x: Decimal = block.get(0, "x")?;
    let ox: Option<Decimal> = block.get(1, "ox")?;
    let ox0: Option<Decimal> = block.get(0, "ox")?;

    assert_eq!(2, block.row_count());
    assert_eq!(1.234, x.into());
    assert_eq!(Some(1.23), ox.map(|v| v.into()));
    assert_eq!(None, ox0);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_column_iter() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE clickhouse_test_column_iter (
            uint64     UInt64,
            str        String,
            fixed_str  FixedString(1),
            opt_str    Nullable(String),
            date       Date,
            datetime   DateTime,
            datetime64 DateTime64(3, 'UTC'),
            decimal    Decimal(8, 3),
            array      Array(UInt32),
            ipv4       IPv4,
            ipv6       IPv6,
            uuid       UUID,
            bln        Bool
        ) Engine=Memory";

    let query = r"SELECT * FROM clickhouse_test_column_iter";

    let date_value: NaiveDate = NaiveDate::from_ymd_opt(2016, 10, 22).unwrap();
    let date_time_value: DateTime<Tz> = UTC.with_ymd_and_hms(2014, 7, 8, 14, 0, 0).unwrap();

    let block = Block::new()
        .column("uint64", vec![1_u64, 2, 3])
        .column("str", vec!["A", "B", "C"])
        .column("fixed_str", vec!["A", "B", "C"])
        .column("opt_str", vec![Some("A"), None, None])
        .column("date", vec![date_value, date_value, date_value])
        .column(
            "datetime",
            vec![date_time_value, date_time_value, date_time_value],
        )
        .column(
            "datetime64",
            vec![date_time_value, date_time_value, date_time_value],
        )
        .column(
            "decimal",
            vec![Decimal::of(1.234, 3), Decimal::of(5, 3), Decimal::of(5, 3)],
        )
        .column("array", vec![vec![42_u32], Vec::new(), Vec::new()])
        .column("ipv4", vec!["127.0.0.1", "127.0.0.1", "127.0.0.1"])
        .column("ipv6", vec!["::1", "::1", "::1"])
        .column(
            "uuid",
            vec![Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap(); 3],
        )
        .column("bln", vec![true, false, true]);

    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_column_iter")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_column_iter", block).await?;

    let mut stream = c.query(query).stream_blocks();

    while let Some(block) = stream.next().await {
        let block = block?;

        let uint64_iter: Vec<_> = block
            .get_column("uint64")?
            .iter::<u64>()?
            .copied()
            .collect();
        assert_eq!(uint64_iter, vec![1_u64, 2, 3]);

        let str_iter: Vec<_> = block.get_column("str")?.iter::<&[u8]>()?.collect();
        assert_eq!(str_iter, vec![&[65_u8], &[66], &[67]]);

        let fixed_str_iter: Vec<_> = block.get_column("fixed_str")?.iter::<&[u8]>()?.collect();
        assert_eq!(fixed_str_iter, vec![&[65_u8], &[66], &[67]]);

        let opt_str_iter: Vec<_> = block
            .get_column("opt_str")?
            .iter::<Option<&[u8]>>()?
            .collect();
        let expected: Vec<Option<&[u8]>> = vec![Some(&[65_u8]), None, None];
        assert_eq!(opt_str_iter, expected);

        let date_iter: Vec<_> = block.get_column("date")?.iter::<NaiveDate>()?.collect();
        assert_eq!(date_iter, vec![date_value, date_value, date_value]);

        let datetime_iter: Vec<_> = block
            .get_column("datetime")?
            .iter::<DateTime<Tz>>()?
            .collect();
        assert_eq!(
            datetime_iter,
            vec![date_time_value, date_time_value, date_time_value]
        );

        let datetime64_iter: Vec<_> = block
            .get_column("datetime64")?
            .iter::<DateTime<Tz>>()?
            .collect();
        assert_eq!(
            datetime64_iter,
            vec![date_time_value, date_time_value, date_time_value]
        );

        let naive_datetime64_iter: Vec<_> = block
            .get_column("datetime64")?
            .iter::<NaiveDateTime>()?
            .collect();
        assert_eq!(
            naive_datetime64_iter,
            vec![
                {
                    let d = NaiveDate::from_ymd_opt(2014, 7, 8).unwrap();
                    let t = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
                    NaiveDateTime::new(d, t)
                };
                3
            ]
        );

        let decimal_iter: Vec<_> = block.get_column("decimal")?.iter::<Decimal>()?.collect();
        assert_eq!(
            decimal_iter,
            vec![Decimal::of(1.234, 3), Decimal::of(5, 3), Decimal::of(5, 3)]
        );

        let array_iter: Vec<_> = block.get_column("array")?.iter::<Vec<u32>>()?.collect();
        assert_eq!(array_iter, vec![vec![&42_u32], Vec::new(), Vec::new()]);

        let ipv4_iter: Vec<_> = block.get_column("ipv4")?.iter::<Ipv4Addr>()?.collect();
        assert_eq!(ipv4_iter, vec![Ipv4Addr::new(127, 0, 0, 1); 3]);

        let ipv6_iter: Vec<_> = block.get_column("ipv6")?.iter::<Ipv6Addr>()?.collect();
        assert_eq!(ipv6_iter, vec![Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1); 3]);

        let uuid_iter: Vec<_> = block.get_column("uuid")?.iter::<Uuid>()?.collect();
        assert_eq!(
            uuid_iter,
            vec![Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap(); 3]
        );

        let bln: Vec<_> = block.get_column("bln")?.iter::<bool>()?.collect();
        assert_eq!(bln, vec![&true, &false, &true]);
    }

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_inconsistent_read() -> Result<(), Error> {
    let pool = Pool::new(database_url());
    let mut client = pool.get_handle().await?;
    {
        let mut stream = client
            .query(
                r"
            SELECT number FROM system.numbers LIMIT 10000
            UNION ALL
            SELECT number FROM system.numbers LIMIT 1000000",
            )
            .stream();

        if let Some(row) = stream.next().await {
            row?;
        }
    }
    client.ping().await?;
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[test]
fn test_reconnect() {
    let url = format!("{}{}", database_url(), "&pool_max=1&pool_min=1");
    let pool = Pool::new(url);

    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..2 {
        let pool = pool.clone();
        let counter = counter.clone();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ret: Result<(), Error> = rt.block_on(async move {
            let mut client = pool.get_handle().await?;

            let block = client.query("SELECT 1").fetch_all().await?;
            let value: u8 = block.get(0, 0)?;
            counter.fetch_add(value as usize, Ordering::SeqCst);

            Ok(())
        });
        ret.unwrap()
    }

    assert_eq!(2, counter.load(Ordering::SeqCst))
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_reusing_handle_after_dropped() -> Result<(), Error> {
    let pool = Pool::new(database_url());

    let mut client = pool.get_handle().await?;

    let some_query = "SELECT * FROM numbers(0,10) UNION ALL SELECT * FROM numbers(0,10)";
    let other_query = "SELECT 1";

    let mut blocks = client.query(some_query).stream_blocks();
    blocks.next().await;
    drop(blocks);

    client.query(other_query).stream().next().await;
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_non_alphanumeric_columns() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_non_alphanumeric_columns (
            `id`  UInt32,
            `ns:exampleAttr1` UInt32,
            `ns:exampleAttr2` UInt32
        ) Engine=Memory
    ";

    let block = Block::new()
        .column("id", vec![1_u32])
        .column("ns:exampleAttr1", vec![2_u32])
        .column("ns:exampleAttr2", vec![3_u32]);

    let pool = Pool::new(database_url());

    let mut client = pool.get_handle().await?;
    client
        .execute("DROP TABLE IF EXISTS clickhouse_non_alphanumeric_columns")
        .await?;
    client.execute(ddl).await?;
    client
        .insert("clickhouse_non_alphanumeric_columns", block)
        .await?;
    let block = client
        .query("SELECT count(*) FROM clickhouse_non_alphanumeric_columns")
        .fetch_all()
        .await?;

    let count: u64 = block.get(0, 0).unwrap();
    assert_eq!(count, 1);
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_ip_from_string() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_test_ipv4 (
            ip_v4 IPv4
        ) ENGINE = Memory
    ";

    let source_block = Block::new().column("ip_v4", vec!["192.168.2.1", "1.2.3.4"]);

    let pool = Pool::new(database_url());

    let mut client = pool.get_handle().await?;
    client
        .execute("DROP TABLE IF EXISTS clickhouse_test_ipv4")
        .await?;
    client.execute(ddl).await?;
    client.insert("clickhouse_test_ipv4", source_block).await?;
    let block = client
        .query("SELECT ip_v4 FROM clickhouse_test_ipv4")
        .fetch_all()
        .await?;

    let ip1: Ipv4Addr = block.get(0, "ip_v4")?;
    let ip2: Ipv4Addr = block.get(1, "ip_v4")?;

    assert_eq!(ip1, Ipv4Addr::new(192, 168, 2, 1));
    assert_eq!(ip2, Ipv4Addr::new(1, 2, 3, 4));
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_ipv6_from_string() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_test_ipv6 (
            ip_v6 IPv6
        ) ENGINE = Memory
    ";

    let source_block = Block::new().column(
        "ip_v6",
        vec!["0001:0203:0405:0607:0809:0A0B:0C0D:0E0F", "::1", "1::"],
    );

    let pool = Pool::new(database_url());

    let mut client = pool.get_handle().await?;
    client
        .execute("DROP TABLE IF EXISTS clickhouse_test_ipv6")
        .await?;
    client.execute(ddl).await?;
    client.insert("clickhouse_test_ipv6", source_block).await?;
    let block = client
        .query("SELECT ip_v6 FROM clickhouse_test_ipv6")
        .fetch_all()
        .await?;

    let ip1: Ipv6Addr = block.get(0, "ip_v6")?;
    let ip2: Ipv6Addr = block.get(1, "ip_v6")?;
    let ip3: Ipv6Addr = block.get(2, "ip_v6")?;

    assert_eq!(
        ip1,
        Ipv6Addr::new(0x0001, 0x0203, 0x0405, 0x0607, 0x0809, 0x0A0B, 0x0C0D, 0x0E0F)
    );
    assert_eq!(ip2, Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1));
    assert_eq!(ip3, Ipv6Addr::new(1, 0, 0, 0, 0, 0, 0, 0));
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_ipv6_db_representation() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_test_ipv6_db_rep (
            ip_v6 IPv6
        ) ENGINE = Memory
    ";

    let source_block = Block::new().column(
        "ip_v6",
        vec!["0001:0203:0405:0607:0809:0A0B:0C0D:0E0F", "::1", "1::"],
    );

    let pool = Pool::new(database_url());

    let mut client = pool.get_handle().await?;
    client
        .execute("DROP TABLE IF EXISTS clickhouse_test_ipv6_db_rep")
        .await?;
    client.execute(ddl).await?;
    client
        .insert("clickhouse_test_ipv6_db_rep", source_block)
        .await?;
    let block = client
        .query("SELECT IPv6NumToString(ip_v6) AS ip_v6_str FROM clickhouse_test_ipv6_db_rep")
        .fetch_all()
        .await?;

    let ip1: &str = block.get(0, "ip_v6_str")?;
    let ip2: &str = block.get(1, "ip_v6_str")?;
    let ip3: &str = block.get(2, "ip_v6_str")?;

    assert_eq!(ip1, "1:203:405:607:809:a0b:c0d:e0f");
    assert_eq!(ip2, "::1");
    assert_eq!(ip3, "1::");
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_insert_date64() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_test_insert_date64 (
            `id` UInt32,
            `date` DateTime64(9, 'UTC')
        ) ENGINE = Memory
    ";

    let date = chrono_tz::UTC
        .with_ymd_and_hms(2020, 2, 3, 13, 45, 50)
        .unwrap()
        .with_nanosecond(8927265)
        .unwrap();
    let mut block = Block::new();
    block.push(row! {
        id: 1u32,
        date,
    })?;

    let pool = Pool::new(database_url());

    let mut client = pool.get_handle().await?;
    client
        .execute("DROP TABLE IF EXISTS clickhouse_test_insert_date64")
        .await?;
    client.execute(ddl).await?;

    client
        .insert("clickhouse_test_insert_date64", &block)
        .await?;

    let result = client
        .query("SELECT * FROM clickhouse_test_insert_date64")
        .fetch_all()
        .await?;
    assert_eq!(result.row_count(), 1);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_simple_agg_func() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_test_simple_agg_func (
            id UInt64,
            val SimpleAggregateFunction(sum, Int64)
        ) ENGINE=AggregatingMergeTree ORDER BY id";

    let mut block = Block::with_capacity(5);
    block.push(row! { id: 1_u64, val:  1_i64 })?;
    block.push(row! { id: 1_u64, val:  2_i64 })?;
    block.push(row! { id: 1_u64, val:  3_i64 })?;
    block.push(row! { id: 2_u64, val:  4_i64 })?;
    block.push(row! { id: 2_u64, val:  5_i64 })?;
    block.push(row! { id: 3_u64, val:  7_i64 })?;

    let pool = Pool::new(database_url());

    let mut client = pool.get_handle().await?;
    client
        .execute("DROP TABLE IF EXISTS clickhouse_test_simple_agg_func")
        .await?;
    client.execute(ddl).await?;
    client
        .insert("clickhouse_test_simple_agg_func", &block)
        .await?;

    let result = client
        .query("SELECT * FROM clickhouse_test_simple_agg_func")
        .fetch_all()
        .await?;
    let actual: Vec<_> = result.get_column("val")?.iter::<i64>()?.copied().collect();
    assert_eq!(actual, vec![6_i64, 9, 7]);
    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_map() -> Result<(), Error> {
    let mut map = HashMap::new();
    map.insert("test".to_string(), 0_u8);
    map.insert("foo".to_string(), 1);

    let b = map.clone();

    let mut block = Block::new();
    block.push(row! {
        id: 1u32,
        map,
    })?;

    let pool = Pool::new(database_url());
    let mut client = pool.get_handle().await?;

    client
        .execute("DROP TABLE IF EXISTS clickhouse_test_map")
        .await
        .unwrap();
    client
        .execute(
            "
        CREATE TABLE IF NOT EXISTS clickhouse_test_map (
            id UInt32,
            map Map(String, UInt8)
        ) Engine=MergeTree ORDER BY id;
         ",
        )
        .await
        .unwrap();

    client.insert("clickhouse_test_map", &block).await.unwrap();

    let result = client
        .query("SELECT * FROM clickhouse_test_map")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(result.row_count(), 1);

    for row in result.rows() {
        let id: u32 = row.get("id")?;
        let map: HashMap<String, u8> = row.get("map")?;

        assert_eq!(id, 1);
        assert_eq!(map, b);
    }

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_iter_map() -> Result<(), Error> {
    let mut block = Block::new();
    block.push(row! {
        map: Value::from(HashMap::from([
            (1_u8, HashMap::from([(3_u8, 5_u8)])),
            (2_u8, HashMap::from([(4_u8, 6_u8), (7, 8)])),
        ])),
        opt_map: Value::Map(
            SqlType::from(Value::UInt8(3).clone()).into(),
            SqlType::from(Value::from(Some(4_u8))).into(),
            Arc::new(HashMap::from([
                (
                    Value::UInt8(3),
                    Value::from(Some(4_u8))
                ),
                (
                    Value::UInt8(5),
                    Value::from(Some(6_u8))
                ),
            ])),
        ),
    })?;

    let pool = Pool::new(database_url());
    let mut client = pool.get_handle().await?;

    client
        .execute("DROP TABLE IF EXISTS clickhouse_test_iter_map")
        .await
        .unwrap();
    client
        .execute(
            r"
            create table clickhouse_test_iter_map
            (
                map Map(UInt8,Map(UInt8,UInt8)),
                opt_map Map(UInt8,Nullable(UInt8))
            )
            engine = Memory;",
        )
        .await
        .unwrap();

    client
        .insert("clickhouse_test_iter_map", &block)
        .await
        .unwrap();

    let result = client
        .query("SELECT * FROM clickhouse_test_iter_map")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(result.row_count(), 1);
    dbg!(result);

    Ok(())
}

#[tokio::test]
async fn test_iter_low_cardinality() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_iter_low_cardinality (
            id       UInt64,
            text     LowCardinality(String),
            fixed    LowCardinality(FixedString(1)),
            datetime LowCardinality(DateTime),
            date     LowCardinality(Date)
        ) ENGINE = Memory
    ";

    let block = Block::new()
        .column("text", vec!["A", "B", "C", "B", "B"])
        .column("fixed", vec!["A", "B", "C", "B", "B"])
        .column("id", vec![1_u64, 2, 3, 4, 5])
        .column(
            "datetime",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            ],
        )
        .column(
            "date",
            vec![
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
            ],
        );

    let options = Options::from_str(&database_url())?.with_setting(
        "allow_suspicious_low_cardinality_types",
        1,
        true,
    );
    let pool = Pool::new(options);

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_iter_low_cardinality")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_iter_low_cardinality", block).await?;
    let query = "SELECT id, text, fixed, datetime, date FROM clickhouse_iter_low_cardinality";
    let block = c.query(query).fetch_all().await?;

    let text_col: Vec<_> = block
        .get_column("text")?
        .iter::<&[u8]>()?
        .filter_map(|s| str::from_utf8(s).ok())
        .collect();
    let fixed_col: Vec<_> = block
        .get_column("fixed")?
        .iter::<&[u8]>()?
        .filter_map(|s| str::from_utf8(s).ok())
        .collect();
    let datetime_col: Vec<_> = block
        .get_column("datetime")?
        .iter::<DateTime<Tz>>()?
        .collect();
    let date_col: Vec<_> = block.get_column("date")?.iter::<NaiveDate>()?.collect();

    assert_eq!(text_col, vec!["A", "B", "C", "B", "B"]);
    assert_eq!(fixed_col, vec!["A", "B", "C", "B", "B"]);
    assert_eq!(
        datetime_col,
        vec![
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
        ]
    );
    assert_eq!(
        date_col,
        vec![
            NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
            NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
            NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
            NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
            NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_low_cardinality() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS clickhouse_low_cardinality (
            id       UInt64,
            text     LowCardinality(String),
            fixed    LowCardinality(FixedString(2)),
            date     LowCardinality(Date),
            datetime LowCardinality(DateTime),
            num      LowCardinality(UInt32)
        ) ENGINE = Memory
    ";

    let block = Block::new()
        .column("id", vec![1_u64, 2, 3, 4, 5])
        .column("num", vec![1_u32, 2, 3, 2, 2])
        .column("text", vec!["A", "B", "C", "B", "B"])
        .column("fixed", vec!["AA", "BB", "CC", "BB", "BB"])
        .column(
            "date",
            vec![
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
                NaiveDate::from_ymd_opt(2016, 10, 22).unwrap(),
            ],
        )
        .column(
            "datetime",
            vec![
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
                UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap(),
            ],
        );

    let options = Options::from_str(&database_url())?.with_setting(
        "allow_suspicious_low_cardinality_types",
        1,
        true,
    );
    let pool = Pool::new(options);

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_low_cardinality")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_low_cardinality", block).await?;

    let query = "SELECT id, text, fixed, date, datetime, num FROM clickhouse_low_cardinality";
    let block = c.query(query).fetch_all().await?;

    let id: u64 = block.get(0, "id")?;
    let text: String = block.get(0, "text")?;
    let fixed: String = block.get(0, "fixed")?;
    let date: NaiveDate = block.get(0, "date")?;
    let datetime: DateTime<Tz> = block.get(0, "datetime")?;
    let num: u32 = block.get(0, "num")?;

    assert_eq!(id, 1_u64);
    assert_eq!(text, "A".to_string());
    assert_eq!(fixed, "AA".to_string());
    assert_eq!(date, NaiveDate::from_ymd_opt(2016, 10, 22).unwrap());
    assert_eq!(
        datetime,
        UTC.with_ymd_and_hms(2016, 10, 22, 12, 0, 0).unwrap()
    );
    assert_eq!(num, 1_u32);

    Ok(())
}

#[tokio::test]
async fn test_int_128() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_test_int_128 (
            i  Int128,
            u UInt128,
            oi Nullable(Int128)
        ) Engine=Memory";

    let query = "SELECT i, u, oi FROM clickhouse_test_int_128";

    let block = Block::new()
        .column("i", vec![1_000_i128, 2_000_000, 3_000_000_000])
        .column("u", vec![1_000_u128, 2_000_000, 3_000_000_000])
        .column("oi", vec![Some(1_000_i128), None, Some(3_000_000_000)]);

    let pool = Pool::new(database_url());

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_int_128")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_int_128", block).await?;
    let block = c.query(query).fetch_all().await?;

    let i: i128 = block.get(0, "i")?;
    let u: u128 = block.get(0, "u")?;
    let oi: Option<i128> = block.get(0, "oi")?;

    assert_eq!(i, 1_000_i128);
    assert_eq!(u, 1_000_u128);
    assert_eq!(oi, Some(1_000_i128));

    Ok(())
}

#[tokio::test]
async fn test_iter_int_128() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_test_iter_int_128 (
            i  Int128,
            u  UInt128,
            oi Nullable(Int128),
            ou Nullable(UInt128)
        ) Engine=Memory";

    let query = "SELECT i, u, oi, ou FROM clickhouse_test_iter_int_128";

    let block = Block::new()
        .column("i", vec![1_000_i128, 2_000_000, 3_000_000_000])
        .column("u", vec![1_000_u128, 2_000_000, 3_000_000_000])
        .column("oi", vec![Some(1_000_i128), None, Some(3_000_000_000)])
        .column("ou", vec![Some(1_000_u128), None, Some(3_000_000_000)]);

    let pool = Pool::new(database_url());

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_iter_int_128")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_iter_int_128", block).await?;
    let block = c.query(query).fetch_all().await?;

    let is: Vec<_> = block.get_column("i")?.iter::<i128>()?.copied().collect();
    assert_eq!(is, vec![1_000_i128, 2_000_000, 3_000_000_000]);

    let us: Vec<_> = block.get_column("u")?.iter::<u128>()?.copied().collect();
    assert_eq!(us, vec![1_000_u128, 2_000_000, 3_000_000_000]);

    let ois: Vec<_> = block.get_column("oi")?.iter::<Option<i128>>()?.collect();
    assert_eq!(ois, vec![Some(&1_000_i128), None, Some(&3_000_000_000)]);

    let ous: Vec<_> = block.get_column("ou")?.iter::<Option<u128>>()?.collect();
    assert_eq!(ous, vec![Some(&1_000_u128), None, Some(&3_000_000_000)]);

    Ok(())
}

#[cfg(feature = "tokio_io")]
#[tokio::test]
async fn test_insert_big_block() -> Result<(), Error> {
    let ddl = r"
               CREATE TABLE clickhouse_test_insert_big_block (
               int8  Int8
               ) Engine=Memory";
    let big_block_size = 1024 * 1024 + 1;

    let block = Block::new().column("int8", vec![-1_i8; big_block_size]);

    let expected = block.clone();
    let pool = Pool::new(database_url());
    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_test_insert_big_block")
        .await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_test_insert_big_block", block).await?;
    let actual = c
        .query("SELECT * FROM clickhouse_test_insert_big_block")
        .fetch_all()
        .await?;

    assert_eq!(format!("{:?}", expected.as_ref()), format!("{:?}", &actual));
    Ok(())
}
