use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(timestamp, {
    let g = get_tester!();

    g.run(
        "
CREATE TABLE TimestampLog (
    id INTEGER,
    t1 TIMESTAMP,
    t2 TIMESTAMP
)",
    )
    .await;

    g.run(
        "
INSERT INTO TimestampLog VALUES
    (1, '2020-06-11 11:23:11Z',           '2021-03-01'),
    (2, '2020-09-30 12:00:00 -07:00',     '1989-01-01T00:01:00+09:00'),
    (3, '2021-04-30T07:00:00.1234-17:00', '2021-05-01T09:00:00.1234+09:00');
",
    )
    .await;

    macro_rules! t {
        ($timestamp: expr) => {
            $timestamp.parse().unwrap()
        };
    }

    g.test(
        "SELECT id, t1, t2 FROM TimestampLog",
        Ok(select!(
            id  | t1                             | t2
            I64 | Timestamp                      | Timestamp;
            1     t!("2020-06-11T11:23:11")        t!("2021-03-01T00:00:00");
            2     t!("2020-09-30T19:00:00")        t!("1988-12-31T15:01:00");
            3     t!("2021-05-01T00:00:00.1234")   t!("2021-05-01T00:00:00.1234")
        )),
    )
    .await;

    g.test(
        "SELECT * FROM TimestampLog WHERE t1 > t2",
        Ok(select!(
            id  | t1                        | t2
            I64 | Timestamp                 | Timestamp;
            2     t!("2020-09-30T19:00:00")   t!("1988-12-31T15:01:00")
        )),
    )
    .await;

    g.test(
        "SELECT * FROM TimestampLog WHERE t1 = t2",
        Ok(select!(
            id  | t1                             | t2
            I64 | Timestamp                      | Timestamp;
            3     t!("2021-05-01T00:00:00.1234")   t!("2021-05-01T00:00:00.1234")
        )),
    )
    .await;

    g.test(
        "SELECT * FROM TimestampLog WHERE t1 = '2020-06-11T14:23:11+0300';",
        Ok(select!(
            id  | t1                        | t2
            I64 | Timestamp                 | Timestamp;
            1     t!("2020-06-11T11:23:11")   t!("2021-03-01T00:00:00")
        )),
    )
    .await;

    g.test(
        "SELECT * FROM TimestampLog WHERE t2 < TIMESTAMP '2000-01-01';",
        Ok(select!(
            id  | t1                        | t2
            I64 | Timestamp                 | Timestamp;
            2     t!("2020-09-30T19:00:00")   t!("1988-12-31T15:01:00")
        )),
    )
    .await;

    g.test(
        "SELECT * FROM TimestampLog WHERE TIMESTAMP '1999-01-03' < '2000-01-01';",
        Ok(select!(
            id  | t1                             | t2
            I64 | Timestamp                      | Timestamp;
            1     t!("2020-06-11T11:23:11")        t!("2021-03-01T00:00:00");
            2     t!("2020-09-30T19:00:00")        t!("1988-12-31T15:01:00");
            3     t!("2021-05-01T00:00:00.1234")   t!("2021-05-01T00:00:00.1234")
        )),
    )
    .await;

    g.test(
        "SELECT id, t1 - t2 AS timestamp_sub FROM TimestampLog;",
        Ok(select!(
            id  | timestamp_sub
            I64 | Interval;
            1     gluesql_core::data::Interval::seconds(-22_682_209);
            2     gluesql_core::data::Interval::seconds(1_001_908_740);
            3     gluesql_core::data::Interval::seconds(0)
        )),
    )
    .await;

    g.test(
        "SELECT
            id,
            t1 - INTERVAL '1' DAY AS sub,
            t2 + INTERVAL '1' MONTH AS add
        FROM TimestampLog;",
        Ok(select!(
            id  | sub                            | add
            I64 | Timestamp                      | Timestamp;
            1     t!("2020-06-10T11:23:11")        t!("2021-04-01T00:00:00");
            2     t!("2020-09-29T19:00:00")        t!("1989-01-31T15:01:00");
            3     t!("2021-04-30T00:00:00.1234")   t!("2021-06-01T00:00:00.1234")
        )),
    )
    .await;

    g.test(
        "INSERT INTO TimestampLog VALUES (1, '12345-678', '2021-05-01')",
        Err(ValueError::FailedToParseTimestamp("12345-678".to_owned()).into()),
    )
    .await;
});
