extern crate chrono;
extern crate chrono_tz;
extern crate clickhouse_rs;
extern crate tokio;

use std::{
    env,
    f64::EPSILON,
    fmt::Debug,
    net::{Ipv4Addr, Ipv6Addr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use chrono::prelude::*;
use chrono_tz::Tz::{self, UCT, UTC};
use tokio::prelude::*;

use clickhouse_rs::{
    errors::{codes, Error},
    types::{Block, Decimal, FromSql, Enum16, Enum8,},
    ClientHandle, Pool,
};
use uuid::Uuid;

type BoxFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;

fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9000?compression=lz4&ping_timeout=2s&retry_timeout=3s".into()
    })
}

/// Same as `tokio::run`, but will panic if future panics and will return the result
/// of future execution.
fn run<F, T, U>(future: F) -> Result<T, U>
where
    F: Future<Item = T, Error = U> + Send + 'static,
    T: Send + 'static,
    U: Send + 'static,
{
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(future);
    runtime.shutdown_on_idle().wait().unwrap();
    result
}

#[test]
fn test_ping() {
    let pool = Pool::new(database_url());
    let done = pool.get_handle().and_then(ClientHandle::ping).map(|_| ());

    run(done).unwrap()
}

#[test]
fn test_connection_by_wrong_address() {
    let pool = Pool::new("tcp://badaddr:9000");
    let done = pool.get_handle().and_then(ClientHandle::ping).map(|_| ());

    run(done).unwrap_err();
}

#[test]
fn test_create_table() {
    let ddl = "\
               CREATE TABLE clickhouse_test_create_table (\
               click_id   FixedString(64), \
               click_time DateTime\
               ) Engine=Memory";

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_test_create_table"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.execute(ddl))
        .map(|_| ());

    let err = run(done).unwrap_err();

    match err {
        Error::Server(err) => assert_eq!(
            err.message,
            "DB::Exception: Table default.clickhouse_test_create_table already exists."
        ),
        _ => panic!("{:?}", err),
    }
}

#[test]
fn test_insert() {
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
               uuid UUID
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
                UCT.ymd(2016, 10, 22),
                UCT.ymd(2016, 10, 22),
                UCT.ymd(2016, 10, 22),
                UCT.ymd(2016, 10, 22),
                UCT.ymd(2016, 10, 22),
                UCT.ymd(2016, 10, 22),
                UCT.ymd(2016, 10, 22),
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
        )
        .column(
            "datetime64",
            vec![
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
                UTC.ymd(2016, 10, 22).and_hms(12, 0, 0),
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
        );

    let expected = block.clone();

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_test_insert"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_insert", block))
        .and_then(move |c| c.query("SELECT * FROM clickhouse_test_insert").fetch_all())
        .map(move |(_, actual)| {
            assert_eq!(format!("{:?}", expected.as_ref()), format!("{:?}", &actual));
        });

    run(done).unwrap()
}

#[test]
fn test_select() {
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
    let done = pool.get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_test_select"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_select", block))
        .and_then(|c| c.query("SELECT COUNT(*) FROM clickhouse_test_select").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(4, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT COUNT(*) FROM clickhouse_test_select WHERE date = '2014-07-08'").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT COUNT(*) FROM clickhouse_test_select WHERE datetime = '2014-07-08 14:00:00'").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(2, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT COUNT(*) FROM clickhouse_test_select WHERE id IN (1, 2, 3)").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT COUNT(*) FROM clickhouse_test_select WHERE code IN ('US', 'DE', 'RU')").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT 1").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(r.row_count(), 1);
            assert_eq!(1, r.get::<i32, _>(0, "id")?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT 1, 2").fetch_all())
        .and_then(|(_, r)| {
            assert_eq!(r.row_count(), 2);
            assert_eq!(2, r.get::<i32, _>(0, "id")?);
            assert_eq!(3, r.get::<i32, _>(1, 0)?);
            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_simple_select() {
    let pool = Pool::new(database_url());
    let done = pool.get_handle()
        .and_then(|c| c.query("SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a) ORDER BY a ASC").fetch_all())
        .and_then(|(c, actual)| {
            let expected = Block::new()
                .column("a", vec![1_u8, 2, 3]);
            assert_eq!(expected, actual);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT min(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(1, r.get::<u8, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT max(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u8, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT sum(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)").fetch_all())
        .and_then(|(c, r)| {
            assert_eq!(6, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query("SELECT median(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)").fetch_all())
        .and_then(|(_, r)| {
            assert!((2_f64 - r.get::<f64, _>(0, 0)?).abs() < EPSILON);
            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_temporary_table() {
    let ddl = "CREATE TEMPORARY TABLE clickhouse_test_temporary_table (ID UInt64);";

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(move |c| c.execute(ddl))
        .and_then(|c| {
            c.execute(
                "INSERT INTO clickhouse_test_temporary_table (ID) \
                 SELECT number AS ID FROM system.numbers LIMIT 10",
            )
        })
        .and_then(|c| {
            c.query("SELECT ID AS ID FROM clickhouse_test_temporary_table")
                .fetch_all()
        })
        .map(|(_, block)| {
            let expected = Block::new().column("ID", (0_u64..10).collect::<Vec<_>>());
            assert_eq!(block, expected)
        });

    run(done).unwrap();
}

#[test]
fn test_with_totals() {
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
        .column("country", vec![2u64, 4, 6]);

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_test_with_totals"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_with_totals", block))
        .and_then(move |c| c.query(query).fetch_all())
        .map(move |(_, block)| assert_eq!(&expected, &block));

    run(done).unwrap();
}

#[test]
fn test_stream_rows() {
    let pool = Pool::new(database_url());

    let done = pool.get_handle().and_then(|c| {
        c.query("SELECT number FROM system.numbers LIMIT 10")
            .stream_rows()
            .fold(0_u64, |acc, row| -> Result<u64, Error> {
                let number: u64 = row.get("number")?;
                Ok(acc + number)
            })
    });

    assert_eq!(45, run(done).unwrap());
}

#[test]
fn test_concurrent_queries() {
    fn query_sum(n: u64) -> BoxFuture<u64> {
        let sql = format!("SELECT number FROM system.numbers LIMIT {}", n);

        let pool = Pool::new(database_url());
        Box::new(
            pool.get_handle()
                .and_then(move |c| {
                    c.query(sql.as_str())
                        .fold(0_u64, |acc, row| Ok(acc + row.get::<u64, _>("number")?))
                })
                .map(|(_, value)| value),
        )
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

    let done = future::join_all(requests).and_then(move |xs| {
        let actual: u64 = xs.iter().sum();
        assert_eq!(actual, expected);
        Ok(())
    });

    run(done).unwrap();
}

#[test]
fn test_big_block() {
    let sql = "SELECT
        number, number, number, number, number, number, number, number, number, number
        FROM system.numbers LIMIT 20000";

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(move |c| c.query(sql).fetch_all())
        .and_then(move |(_, block)| Ok(block.row_count()));

    let actual = run(done).unwrap();
    assert_eq!(actual, 20000)
}

#[test]
fn test_nullable() {
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
            uuid       Nullable(UUID)
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
            uuid
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
        .column("datetime", vec![Some(date_time_value)])
        .column("datetime64", vec![Some(date_time_value)])
        .column("ipv4", vec![Some(Ipv4Addr::new(127, 0, 0, 1))])
        .column(
            "ipv6",
            vec![Some(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff))],
        )
        .column("uuid", vec![Some(Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap())]);

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_test_nullable"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_nullable", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
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
            let datetime64: Option<DateTime<Tz>> = block.get(0, "datetime64")?;
            let ipv4: Option<Ipv4Addr> = block.get(0, "ipv4")?;
            let ipv6: Option<Ipv6Addr> = block.get(0, "ipv6")?;
            let uuid: Option<Uuid> = block.get(0, "uuid")?;

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

            assert_eq!(uuid, Some(Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap()));

            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_generic_column() {
    fn extract_to_vec<'a, T>(name: &str, block: &'a Block) -> Vec<T>
    where
        T: FromSql<'a> + Debug + 'static,
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

#[test]
fn test_fixed_string() {
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
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_test_fixed_string"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_fixed_string", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
            let text: &str = block.get(0, "text")?;
            let opt_text: Option<&str> = block.get(0, "opt_text")?;

            assert_eq!(text, "text");
            assert_eq!(opt_text, Some("text"));

            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_binary_string() {
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
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_binary_string"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_binary_string", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
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
        });

    run(done).unwrap();
}

#[test]
fn test_enum_16_not_nullable() {
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
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_enum16_non_nul"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_enum16_non_nul", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
            let enum_16_a: Enum16 = block.get(0, "enum_16_row")?;
            let enum_16_b: Enum16 = block.get(1, "enum_16_row")?;

            assert_eq!(2, block.row_count());
            assert_eq!(
                vec!([Enum16::of(5), Enum16::of(6)]),
                vec!([enum_16_a, enum_16_b])
            );

            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_enum_16_nullable() {
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
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_enum"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_enum", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
            let enum_16_a: Option<Enum16> = block.get(0, "enum_16_row")?;
            let enum_16_b: Option<Enum16> = block.get(1, "enum_16_row")?;

            assert_eq!(3, block.row_count());
            assert_eq!(
                vec!([Some(Enum16::of(5)), Some(Enum16::of(6))]),
                vec!([enum_16_a, enum_16_b])
            );

            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_enum8() {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_Enum (
            enum_8_row        Enum8(
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
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_Enum"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_Enum", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
            let enum_8_a: Enum8 = block.get(0, "enum_8_row")?;
            let enum_8_b: Enum8 = block.get(1, "enum_8_row")?;

            assert_eq!(2, block.row_count());
            assert_eq!(
                vec!([Enum8::of(1), Enum8::of(2)]),
                vec!([enum_8_a, enum_8_b])
            );

            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_array() {
    let ddl = "
        CREATE TABLE clickhouse_array (
            u8    Array(UInt8),
            u32   Array(UInt32),
            f64   Array(Float64),
            text1 Array(String),
            text2 Array(String),
            date  Array(Date),
            time  Array(DateTime),
            time64  Array(DateTime64(3, 'UTC'))
        ) Engine=Memory";

    let query = "SELECT u8, u32, f64, text1, text2, date, time, time64  FROM clickhouse_array";

    let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
    let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

    let block = Block::new()
        .column("u8", vec![vec![41_u8]])
        .column("u32", vec![vec![42_u32]])
        .column("f64", vec![vec![42_f64]])
        .column("text1", vec![vec!["A"]])
        .column("text2", vec![vec!["B".to_string()]])
        .column("date", vec![vec![date_value]])
        .column("time", vec![vec![date_time_value]])
        .column("time64", vec![vec![date_time_value]]);

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_array"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_array", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
            let u8_vec: Vec<u8> = block.get(0, "u8")?;
            let u32_vec: Vec<u32> = block.get(0, "u32")?;
            let f64_vec: Vec<f64> = block.get(0, "f64")?;
            let text1_vec: Vec<&str> = block.get(0, "text1")?;
            let text2_vec: Vec<String> = block.get(0, "text2")?;
            let date_vec: Vec<Date<Tz>> = block.get(0, "date")?;
            let time_vec: Vec<DateTime<Tz>> = block.get(0, "time")?;
            let time64_vec: Vec<DateTime<Tz>> = block.get(0, "time64")?;

            assert_eq!(1, block.row_count());
            assert_eq!(vec![41_u8], u8_vec);
            assert_eq!(vec![42_u32], u32_vec);
            assert_eq!(vec![42_f64], f64_vec);
            assert_eq!(vec!["A"], text1_vec);
            assert_eq!(vec!["B".to_string()], text2_vec);
            assert_eq!(vec![date_value], date_vec);
            assert_eq!(vec![date_time_value], time_vec);
            assert_eq!(vec![date_time_value], time64_vec);

            Ok(())
        });

    run(done).unwrap();
}

#[test]
#[allow(clippy::float_cmp)]
fn test_decimal() {
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
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_decimal"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_decimal", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
            let x: Decimal = block.get(0, "x")?;
            let ox: Option<Decimal> = block.get(1, "ox")?;
            let ox0: Option<Decimal> = block.get(0, "ox")?;

            assert_eq!(2, block.row_count());
            assert_eq!(1.234, x.into());
            assert_eq!(Some(1.23), ox.map(|v| v.into()));
            assert_eq!(None, ox0);

            Ok(())
        });

    run(done).unwrap();
}

#[test]
fn test_reconnect() {
    let counter = Arc::new(AtomicUsize::new(0));

    let url = format!("{}{}", database_url(), "&pool_max=1&pool_min=1");
    let pool = Pool::new(url);

    for _ in 0..2 {
        let counter = counter.clone();
        let done = pool
            .get_handle()
            .and_then(move |c| c.query("SELECT 1").fetch_all())
            .and_then(move |(_, block)| {
                let value: u8 = block.get(0, 0)?;
                counter.fetch_add(value as usize, Ordering::SeqCst);
                Ok(())
            })
            .map_err(|err| eprintln!("database error: {}", err));

        run(done).unwrap();
    }

    assert_eq!(2, counter.load(Ordering::SeqCst))
}

#[test]
fn test_column_iter() {
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
            uuid       UUID
        ) Engine=Memory";

    let query = r"SELECT * FROM clickhouse_test_column_iter";

    let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
    let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

    let block = Block::new()
        .add_column("uint64", vec![1_u64, 2, 3])
        .add_column("str", vec!["A", "B", "C"])
        .add_column("fixed_str", vec!["A", "B", "C"])
        .add_column("opt_str", vec![Some("A"), None, None])
        .add_column("date", vec![date_value, date_value, date_value])
        .add_column(
            "datetime",
            vec![date_time_value, date_time_value, date_time_value],
        )
        .column(
            "datetime64",
            vec![date_time_value, date_time_value, date_time_value],
        )
        .add_column(
            "decimal",
            vec![Decimal::of(1.234, 3), Decimal::of(5, 3), Decimal::of(5, 3)],
        )
        .add_column("array", vec![vec![42_u32], Vec::new(), Vec::new()])
        .column("ipv4", vec!["127.0.0.1", "127.0.0.1", "127.0.0.1"])
        .column("ipv6", vec!["::1", "::1", "::1"])
        .column("uuid", vec![Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap(); 3]);

    let pool = Pool::new(database_url());

    let done = pool
        .get_handle()
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_test_column_iter"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_column_iter", block))
        .and_then(move |c| {
            c.query(query).stream_blocks().for_each(move |block| {
                let uint64_iter: Vec<_> = block
                    .get_column("uint64")?
                    .iter::<u64>()?
                    .copied()
                    .collect();
                assert_eq!(uint64_iter, vec![1_u64, 2, 3]);

                let str_iter: Vec<_> = block.get_column("str")?.iter::<&[u8]>()?.collect();
                assert_eq!(str_iter, vec![&[65_u8], &[66], &[67]]);

                let fixed_str_iter: Vec<_> =
                    block.get_column("fixed_str")?.iter::<&[u8]>()?.collect();
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

                let datetime64_iter: Vec<_> = block
                    .get_column("datetime64")?
                    .iter::<DateTime<Tz>>()?
                    .collect();
                assert_eq!(
                    datetime64_iter,
                    vec![date_time_value, date_time_value, date_time_value]
                );

                let decimal_iter: Vec<_> =
                    block.get_column("decimal")?.iter::<Decimal>()?.collect();
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
                assert_eq!(uuid_iter, vec![Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap(); 3]);

                Ok(())
            })
        });

    run(done).unwrap();
}

#[test]
fn test_non_alphanumeric_columns() {
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
    let done = pool
        .get_handle()
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_non_alphanumeric_columns"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_non_alphanumeric_columns", block))
        .and_then(move |c| {
            c.query("SELECT count(*) FROM clickhouse_non_alphanumeric_columns")
                .fetch_all()
        })
        .map(move |(_, block)| {
            let count: u64 = block.get(0, 0).unwrap();
            assert_eq!(count, 1)
        });

    run(done).unwrap()
}

#[test]
fn test_syntax_error() {
    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(move |c| c.query("SYNTAX ERROR").fetch_all())
        .map(move |_| panic!())
        .map_err(|err| {
            if let Error::Server(e) = err {
                assert_eq!(e.code, codes::SYNTAX_ERROR);
                assert!(e.handle.is_some());
            } else {
                panic!();
            }
        });

    run(done).unwrap_err()
}

#[test]
fn test_ip_from_string() {
    let ddl = "
        CREATE TABLE IF NOT EXISTS clickhouse_test_ipv4 (
            ip_v4 IPv4
        ) ENGINE = Memory
    ";

    let source_block = Block::new()
        .column("ip_v4", vec!["192.168.2.1", "1.2.3.4"]);

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_test_ipv4"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_ipv4", source_block))
        .and_then(move |c| c.query("SELECT ip_v4 FROM clickhouse_test_ipv4").fetch_all())
        .and_then(move |(_, block)| Ok(block));

    let block = run(done).unwrap();

    let ip1: Ipv4Addr = block.get(0, "ip_v4").unwrap();
    let ip2: Ipv4Addr = block.get(1, "ip_v4").unwrap();

    assert_eq!(ip1, Ipv4Addr::new(192, 168, 2, 1));
    assert_eq!(ip2, Ipv4Addr::new(1, 2, 3, 4));
}


#[test]
fn test_city_hash_128() {
    let expected = 0x900ff195577748fe13a9176355b20d7eu128;
    let actual = cityhash_rs::cityhash_102_128("abc".as_bytes());
    assert_eq!(expected, actual)
}
