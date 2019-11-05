use std::{env, f64::EPSILON, fmt, sync::{Arc, atomic::{Ordering, AtomicUsize}}};

use chrono::prelude::*;
use chrono_tz::Tz;
use futures_util::{future, stream::StreamExt, try_stream::TryStreamExt};

use clickhouse_rs::{
    errors::Error,
    types::{Decimal, FromSql},
    Block, Pool,
};
use Tz::UTC;

fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into())
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
        assert_eq!(
            "Server error: `ERROR DB::Exception (57): DB::Exception: Table default.clickhouse_test_create_table already exists.`",
            format!("{}", err)
        );
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
               datetime DateTime
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
                UTC.ymd(2016, 10, 22),
                UTC.ymd(2016, 10, 22),
                UTC.ymd(2016, 10, 22),
                UTC.ymd(2016, 10, 22),
                UTC.ymd(2016, 10, 22),
                UTC.ymd(2016, 10, 22),
                UTC.ymd(2016, 10, 22),
            ],
        )
        .column(
            "datetime",
            vec![
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
            ],
        );

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

    assert_eq!(expected.as_ref(), &actual);
    Ok(())
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
                UTC.ymd(2014, 7, 8),
                UTC.ymd(2014, 7, 8),
                UTC.ymd(2014, 7, 8),
                UTC.ymd(2014, 7, 9),
            ],
        )
        .column(
            "datetime",
            vec![
                Tz::Singapore.ymd(2014, 7, 8).and_hms(14, 0, 0),
                UTC.ymd(2014, 7, 8).and_hms(14, 0, 0),
                UTC.ymd(2014, 7, 8).and_hms(14, 0, 0),
                UTC.ymd(2014, 7, 8).and_hms(13, 0, 0),
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
async fn test_simple_select() -> Result<(), Error> {
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
    assert!((2_f64 - r.get::<f64, _>(0, 0)?).abs() < EPSILON);

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
        let sql = format!("SELECT number FROM system.numbers LIMIT {}", n);

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
            int8     Nullable(Int8),
            int16    Nullable(Int16),
            int32    Nullable(Int32),
            int64    Nullable(Int64),
            uint8    Nullable(UInt8),
            uint16   Nullable(UInt16),
            uint32   Nullable(UInt32),
            uint64   Nullable(UInt64),
            float32  Nullable(Float32),
            float64  Nullable(Float64),
            string   Nullable(String),
            date     Nullable(Date),
            datetime Nullable(DateTime)
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
            datetime
        FROM clickhouse_test_nullable";

    let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
    let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

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
        .column("datetime", vec![Some(date_time_value)]);

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
    let date: Option<Date<Tz>> = block.get(0, "date")?;
    let datetime: Option<DateTime<Tz>> = block.get(0, "datetime")?;

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
async fn test_array() -> Result<(), Error> {
    let ddl = "
        CREATE TABLE clickhouse_array (
            u8    Array(UInt8),
            u32   Array(UInt32),
            text1 Array(String),
            text2 Array(String),
            date  Array(Date),
            time  Array(DateTime)
        ) Engine=Memory";

    let query = "SELECT u8, u32, text1, text2, date, time FROM clickhouse_array";

    let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
    let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

    let block = Block::new()
        .column("u8", vec![vec![41_u8]])
        .column("u32", vec![vec![42_u32]])
        .column("text1", vec![vec!["A"]])
        .column("text2", vec![vec!["B".to_string()]])
        .column("date", vec![vec![date_value]])
        .column("time", vec![vec![date_time_value]]);

    let pool = Pool::new(database_url());

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS clickhouse_array").await?;
    c.execute(ddl).await?;
    c.insert("clickhouse_array", block).await?;
    let block = c.query(query).fetch_all().await?;

    let u8_vec: Vec<u8> = block.get(0, "u8")?;
    let u32_vec: Vec<u32> = block.get(0, "u32")?;
    let text1_vec: Vec<&str> = block.get(0, "text1")?;
    let text2_vec: Vec<String> = block.get(0, "text2")?;
    let date_vec: Vec<Date<Tz>> = block.get(0, "date")?;
    let time_vec: Vec<DateTime<Tz>> = block.get(0, "time")?;

    assert_eq!(1, block.row_count());
    assert_eq!(vec![41_u8], u8_vec);
    assert_eq!(vec![42_u32], u32_vec);
    assert_eq!(vec!["A"], text1_vec);
    assert_eq!(vec!["B".to_string()], text2_vec);
    assert_eq!(vec![date_value], date_vec);
    assert_eq!(vec![date_time_value], time_vec);

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
#[tokio::test]
async fn test_column_iter() -> Result<(), Error> {
    let ddl = r"
        CREATE TABLE clickhouse_test_column_iter (
            uint64    UInt64,
            str       String,
            fixed_str FixedString(1),
            opt_str   Nullable(String),
            date      Date,
            datetime  DateTime,
            decimal   Decimal(8, 3),
            array     Array(UInt32)
        ) Engine=Memory";

    let query = r"SELECT * FROM clickhouse_test_column_iter";

    let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
    let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

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
            "decimal",
            vec![Decimal::of(1.234, 3), Decimal::of(5, 3), Decimal::of(5, 3)],
        )
        .column("array", vec![vec![42_u32], Vec::new(), Vec::new()]);

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

        let date_iter: Vec<_> = block.get_column("date")?.iter::<Date<Tz>>()?.collect();
        assert_eq!(date_iter, vec![date_value, date_value, date_value]);

        let datetime_iter: Vec<_> = block
            .get_column("datetime")?
            .iter::<DateTime<Tz>>()?
            .collect();
        assert_eq!(
            datetime_iter,
            vec![date_time_value, date_time_value, date_time_value]
        );

        let decimal_iter: Vec<_> = block.get_column("decimal")?.iter::<Decimal>()?.collect();
        assert_eq!(
            decimal_iter,
            vec![Decimal::of(1.234, 3), Decimal::of(5, 3), Decimal::of(5, 3)]
        );

        let array_iter: Vec<_> = block.get_column("array")?.iter::<Vec<u32>>()?.collect();
        assert_eq!(array_iter, vec![vec![&42_u32], Vec::new(), Vec::new()]);
    }

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
