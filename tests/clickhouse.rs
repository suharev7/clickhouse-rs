extern crate chrono;
extern crate chrono_tz;
extern crate clickhouse_rs;
extern crate tokio;

use std::{env, f64::EPSILON, fmt::Debug};

use chrono::prelude::*;
use chrono_tz::Tz::{self, UTC};
use tokio::prelude::*;

use clickhouse_rs::{errors::Error, types::Block, types::FromSql, ClientHandle, Pool};

type BoxFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;

fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into())
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
    let ddl = "\
               CREATE TABLE clickhouse_test_insert (\
               int8  Int8, \
               int16 Int16, \
               int32 Int32, \
               int64 Int64, \
               uint8  UInt8, \
               uint16 UInt16, \
               uint32 UInt32, \
               uint64 UInt64, \
               float32 Float32, \
               float64 Float64, \
               string  String, \
               date    Date, \
               datetime DateTime \
               ) Engine=Memory";

    let block = Block::new()
        .add_column("int8", vec![-1_i8, -2, -3, -4, -5, -6, -7])
        .add_column("int16", vec![-1_i16, -2, -3, -4, -5, -6, -7])
        .add_column("int32", vec![-1_i32, -2, -3, -4, -5, -6, -7])
        .add_column("int64", vec![-1_i64, -2, -3, -4, -5, -6, -7])
        .add_column("uint8", vec![1_u8, 2, 3, 4, 5, 6, 7])
        .add_column("uint16", vec![1_u16, 2, 3, 4, 5, 6, 7])
        .add_column("uint32", vec![1_u32, 2, 3, 4, 5, 6, 7])
        .add_column("uint64", vec![1_u64, 2, 3, 4, 5, 6, 7])
        .add_column("float32", vec![1.0_f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        .add_column("float64", vec![1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        .add_column("string", vec!["1", "2", "3", "4", "5", "6", "7"])
        .add_column(
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
        .add_column(
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
    let done = pool
        .get_handle()
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_test_insert"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_insert", block))
        .and_then(move |c| c.query("SELECT * FROM clickhouse_test_insert").fetch_all())
        .map(move |(_, actual)| assert_eq!(expected.as_ref(), &actual));

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
        .add_column("id", vec![1, 2, 3, 4])
        .add_column("code", vec!["RU", "UA", "DE", "US"])
        .add_column(
            "date",
            vec![
                UTC.ymd(2014, 7, 8),
                UTC.ymd(2014, 7, 8),
                UTC.ymd(2014, 7, 8),
                UTC.ymd(2014, 7, 9),
            ],
        )
        .add_column(
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
                .add_column("a", vec![1_u8, 2, 3]);
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
            let expected = Block::new().add_column("ID", (0_u64..10).collect::<Vec<_>>());
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

    let block = Block::new().add_column("country", vec!["RU", "EN", "RU", "RU", "EN", "RU"]);

    let expected = Block::new()
        .add_column("country", vec!["EN", "RU", ""])
        .add_column("country", vec![2u64, 4, 6]);

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
        .add_column("int8", vec![Some(1_i8)])
        .add_column("int16", vec![Some(1_i16)])
        .add_column("int32", vec![Some(1_i32)])
        .add_column("int64", vec![Some(1_i64)])
        .add_column("uint8", vec![Some(1_u8)])
        .add_column("uint16", vec![Some(1_u16)])
        .add_column("uint32", vec![Some(1_u32)])
        .add_column("uint64", vec![Some(1_u64)])
        .add_column("float32", vec![Some(1_f32)])
        .add_column("float64", vec![Some(1_f64)])
        .add_column("string", vec![Some("text")])
        .add_column("date", vec![Some(date_value)])
        .add_column("datetime", vec![Some(date_time_value)]);

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
        .add_column("int", vec![1u32, 2, 3])
        .add_column("str", vec!["A", "B", "C"]);

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
        .add_column("opt_text", vec![Some("text")])
        .add_column("text", vec!["text"]);

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
        .add_column("text", vec![vec![0_u8, 159, 146, 150]])
        .add_column("fx_text", vec![vec![0_u8, 159, 146, 150]])
        .add_column("opt_text", vec![Some(vec![0_u8, 159, 146, 150])])
        .add_column("fx_opt_text", vec![Some(vec![0_u8, 159, 146, 150])]);

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
fn test_array() {
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
        .add_column("u8", vec![vec![41_u8]])
        .add_column("u32", vec![vec![42_u32]])
        .add_column("text1", vec![vec!["A"]])
        .add_column("text2", vec![vec!["B".to_string()]])
        .add_column("date", vec![vec![date_value]])
        .add_column("time", vec![vec![date_time_value]]);

    let pool = Pool::new(database_url());
    let done = pool
        .get_handle()
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_array"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_array", block))
        .and_then(move |c| c.query(query).fetch_all())
        .and_then(move |(_, block)| {
            let a: Vec<u8> = block.get(0, "u8")?;
            let b: Vec<u32> = block.get(0, "u32")?;
            let c: Vec<&str> = block.get(0, "text1")?;
            let d: Vec<String> = block.get(0, "text2")?;
            let e: Vec<Date<Tz>> = block.get(0, "date")?;
            let f: Vec<DateTime<Tz>> = block.get(0, "time")?;

            assert_eq!(1, block.row_count());
            assert_eq!(vec![41_u8], a);
            assert_eq!(vec![42_u32], b);
            assert_eq!(vec!["A"], c);
            assert_eq!(vec!["B".to_string()], d);
            assert_eq!(vec![date_value], e);
            assert_eq!(vec![date_time_value], f);

            Ok(())
        });

    run(done).unwrap();
}
