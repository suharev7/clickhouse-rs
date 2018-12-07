extern crate chrono;
extern crate chrono_tz;
extern crate clickhouse_rs;
extern crate tokio;

use chrono::prelude::*;
use chrono_tz::Tz;
use tokio::prelude::*;

use clickhouse_rs::{Block, Client, Options, IoFuture};
use Tz::UTC;

#[test]
fn test_ping() {
    let options = Options::new("127.0.0.1:9000".parse().unwrap());
    let done = Client::connect(options)
        .and_then(|c| c.ping())
        .and_then(|_| Ok(()))
        .map_err(|err| panic!("Exception: {}", err));

    tokio::run(done)
}

#[test]
fn test_create_table() {
    let ddl = "\
               CREATE TABLE clickhouse_test_create_table (\
               click_id   FixedString(64), \
               click_time DateTime\
               ) Engine=Memory";

    let options = Options::new("127.0.0.1:9000".parse().unwrap());
    let done = Client::connect(options)
        .and_then(|c| c.ping())
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_test_create_table"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.execute(ddl))
        .and_then(|_| Ok(()))
        .map_err(|err: ::std::io::Error| {
            assert_eq!(
                "DB::Exception: Table default.clickhouse_test_create_table already exists.",
                format!("{}", err)
            )
        });

    tokio::run(done)
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
        ).add_column(
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

    let options = Options::new("127.0.0.1:9000".parse().unwrap());
    let done = Client::connect(options)
        .and_then(|c| c.ping())
        .and_then(move |c| c.execute("DROP TABLE IF EXISTS clickhouse_test_insert"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_insert", block))
        .and_then(move |c| c.query_all("SELECT * FROM clickhouse_test_insert"))
        .and_then(move |(_, actual)| Ok(assert_eq!(expected.as_ref(), &actual)))
        .map_err(|err| panic!("Exception: {}", err));

    tokio::run(done)
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
        ).add_column(
            "datetime",
            vec![
                Tz::Singapore.ymd(2014, 7, 8).and_hms(14, 0, 0),
                UTC.ymd(2014, 7, 8).and_hms(14, 0, 0),
                UTC.ymd(2014, 7, 8).and_hms(14, 0, 0),
                UTC.ymd(2014, 7, 8).and_hms(13, 0, 0),
            ],
        );

    let options = Options::new("127.0.0.1:9000".parse().unwrap());
    let done = Client::connect(options)
        .and_then(|c| c.ping())
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_test_select"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_select", block))
        .and_then(|c| c.query_all("SELECT COUNT(*) FROM clickhouse_test_select"))
        .and_then(|(c, r)| {
            assert_eq!(4, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT COUNT(*) FROM clickhouse_test_select WHERE date = '2014-07-08'"))
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT COUNT(*) FROM clickhouse_test_select WHERE datetime = '2014-07-08 14:00:00'"))
        .and_then(|(c, r)| {
            assert_eq!(2, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT COUNT(*) FROM clickhouse_test_select WHERE id IN (1, 2, 3)"))
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT COUNT(*) FROM clickhouse_test_select WHERE code IN ('US', 'DE', 'RU')"))
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT 1"))
        .and_then(|(c, r)| {
            assert_eq!(r.row_count(), 1);
            assert_eq!(1, r.get::<i32, _>(0, "id")?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT 1, 2"))
        .and_then(|(_, r)| {
            assert_eq!(r.row_count(), 2);
            assert_eq!(2, r.get::<i32, _>(0, "id")?);
            assert_eq!(3, r.get::<i32, _>(1, 0)?);
            Ok(())
        })
        .map_err(|err| panic!("Exception: {}", err));

    tokio::run(done);
}

#[test]
fn test_simple_select() {
    let options = Options::new("127.0.0.1:9000".parse().unwrap());
    let done = Client::connect(options)
        .and_then(|c| c.ping())
        .and_then(|c| c.query_all("SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a) ORDER BY a ASC"))
        .and_then(|(c, actual)| {
            let expected = Block::new()
                .add_column("a", vec![1u8, 2, 3]);
            assert_eq!(expected, actual);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT min(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"))
        .and_then(|(c, r)| {
            assert_eq!(1, r.get::<u8, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT max(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"))
        .and_then(|(c, r)| {
            assert_eq!(3, r.get::<u8, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT sum(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"))
        .and_then(|(c, r)| {
            assert_eq!(6, r.get::<u64, _>(0, 0)?);
            Ok(c)
        })
        .and_then(|c| c.query_all("SELECT median(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"))
        .and_then(|(_, r)| {
            assert_eq!(2f64, r.get::<f64, _>(0, 0)?);
            Ok(())
        })
        .map_err(|err| panic!("Exception: {}", err));

    tokio::run(done);
}

#[test]
fn test_temporary_table() {
    let ddl = "CREATE TEMPORARY TABLE clickhouse_test_temporary_table (ID UInt64);";

    let options = Options::new("127.0.0.1:9000".parse().unwrap());
    let done = Client::connect(options)
        .and_then(|c| c.ping())
        .and_then(move |c| c.execute(ddl))
        .and_then(|c| {
            c.execute(
                "INSERT INTO clickhouse_test_temporary_table (ID) \
                 SELECT number AS ID FROM system.numbers LIMIT 10",
            )
        }).and_then(|c| c.query_all("SELECT ID AS ID FROM clickhouse_test_temporary_table"))
        .and_then(|(_, block)| {
            let expected = Block::new().add_column("ID", (0u64..10).collect::<Vec<_>>());
            Ok(assert_eq!(block, expected))
        }).map_err(|err| panic!("Exception: {}", err));

    tokio::run(done);
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

    let options = Options::new("127.0.0.1:9000".parse().unwrap());
    let done = Client::connect(options)
        .and_then(|c| c.ping())
        .and_then(|c| c.execute("DROP TABLE IF EXISTS clickhouse_test_with_totals"))
        .and_then(move |c| c.execute(ddl))
        .and_then(move |c| c.insert("clickhouse_test_with_totals", block))
        .and_then(move |c| c.query_all(query))
        .and_then(move |(_, block)| Ok(assert_eq!(&expected, &block)))
        .map_err(|err| panic!("Exception: {}", err));

    tokio::run(done);
}

#[test]
fn test_concurrent_queries() {
    fn query_sum(n: u64) -> IoFuture<u64> {
        let sql = format!("SELECT number FROM system.numbers LIMIT {}", n);

        let options = Options::new("127.0.0.1:9000".parse().unwrap());
        Box::new(
            Client::connect(options)
                .and_then(move |c| c.ping())
                .and_then(move |c| c.query_all(sql.as_str()))
                .and_then(move |(_, block)| {
                    let mut total = 0_u64;
                    for row in 0_usize..block.row_count() {
                        let x: u64 = block.get(row, "number")?;
                        total += x;
                    }
                    Ok(total)
                }),
        )
    }

    let m = 250000_u64;

    let expected = (m*1) * ((m*1) - 1) / 2
        + (m*2) * ((m*2) - 1) / 2
        + (m*3) * ((m*3) - 1) / 2
        + (m*4) * ((m*4) - 1) / 2;

    let requests = vec![
        query_sum(m*1),
        query_sum(m*2),
        query_sum(m*3),
        query_sum(m*4),
    ];

    let done = future::join_all(requests)
        .and_then(move |xs| {
            let actual: u64 = xs.iter().sum();
            assert_eq!(actual, expected);
            Ok(())
        }).map_err(|_| eprintln!("database error"));

    tokio::run(done)
}