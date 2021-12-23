use {
    crate::*,
    data::Interval as I,
    data::ValueError,
    executor::Payload,
    prelude::Value::{self, *},
};

test_case!(cast_literal, async move {
    use chrono::{NaiveDate, NaiveTime};

    let test_cases = vec![
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        (r#"INSERT INTO Item VALUES ("1")"#, Ok(Payload::Insert(1))),
        (
            r#"SELECT CAST("TRUE" AS BOOLEAN) AS cast FROM Item"#,
            Ok(select!(cast Bool; true)),
        ),
        (
            r#"SELECT CAST(1 AS BOOLEAN) AS cast FROM Item"#,
            Ok(select!(cast Bool; true)),
        ),
        (
            r#"SELECT CAST("asdf" AS BOOLEAN) AS cast FROM Item"#,
            Err(ValueError::LiteralCastToBooleanFailed("asdf".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(3 AS BOOLEAN) AS cast FROM Item"#,
            Err(ValueError::LiteralCastToBooleanFailed("3".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(NULL AS BOOLEAN) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST("1" AS INTEGER) AS cast FROM Item"#,
            Ok(select!(cast I64; 1)),
        ),
        (
            r#"SELECT CAST("foo" AS INTEGER) AS cast FROM Item"#,
            Err(ValueError::LiteralCastFromTextToIntegerFailed("foo".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(1.1 AS INTEGER) AS cast FROM Item"#,
            Ok(select!(cast I64; 1)),
        ),
        (
            r#"SELECT CAST(TRUE AS INTEGER) AS cast FROM Item"#,
            Ok(select!(cast I64; 1)),
        ),
        (
            r#"SELECT CAST(NULL AS INTEGER) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST("1.1" AS FLOAT) AS cast FROM Item"#,
            Ok(select!(cast F64; 1.1)),
        ),
        (
            r#"SELECT CAST(1 AS FLOAT) AS cast FROM Item"#,
            Ok(select!(cast F64; 1.0)),
        ),
        (
            r#"SELECT CAST("foo" AS FLOAT) AS cast FROM Item"#,
            Err(ValueError::LiteralCastFromTextToFloatFailed("foo".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(TRUE AS FLOAT) AS cast FROM Item"#,
            Ok(select!(cast F64; 1.0)),
        ),
        (
            r#"SELECT CAST(NULL AS FLOAT) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST(1 AS TEXT) AS cast FROM Item"#,
            Ok(select!(cast Str; "1".to_string())),
        ),
        (
            r#"SELECT CAST(1.1 AS TEXT) AS cast FROM Item"#,
            Ok(select!(cast Str; "1.1".to_string())),
        ),
        (
            r#"SELECT CAST(TRUE AS TEXT) AS cast FROM Item"#,
            Ok(select!(cast Str; "TRUE".to_string())),
        ),
        (
            r#"SELECT CAST(NULL AS TEXT) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST(NULL AS INTERVAL) FROM Item"#,
            Err(ValueError::UnimplementedLiteralCast {
                data_type: ast::DataType::Interval,
                literal: format!("{:?}", data::Literal::Null),
            }
            .into()),
        ),
        (
            r#"SELECT
            CAST("'1-2' YEAR TO MONTH" as INTERVAL) as stoi_1,
            CAST("'12' DAY" as INTERVAL) as stoi_2,
            CAST("'12' MINUTE" as INTERVAL) as stoi_3,
            CAST("'-3 14' DAY TO HOUR" as INTERVAL) as stoi_4,
            CAST("'3 14:00:00' DAY TO SECOND" as INTERVAL) as stoi_5,
            CAST("'12:00' HOUR TO MINUTE" as INTERVAL) as stoi_6,
            CAST("'-1000-11' YEAR TO MONTH" as INTERVAL) as stoi_7,
            CAST("'30' MONTH" as INTERVAL) as stoi_8,
            CAST("'35' HOUR" as INTERVAL) as stoi_9,
            CAST("'300' SECOND" as INTERVAL) as stoi_10,
            CAST("'3 12:30' DAY TO MINUTE" as INTERVAL) as stoi_11,
            CAST("'3 12:30:12.1324' DAY TO SECOND" as INTERVAL) as stoi_12,
            CAST("'-12:30:12' HOUR TO SECOND" as INTERVAL) as stoi_13,
            CAST("'-30:11' MINUTE TO SECOND" as INTERVAL) as stoi_14
            FROM Item"#,
            Ok(select!(
            stoi_1|stoi_2|stoi_3|stoi_4|stoi_5|stoi_6|stoi_7|stoi_8|stoi_9|stoi_10|stoi_11|stoi_12|stoi_13|stoi_14
            Interval|Interval|Interval|Interval|Interval|Interval|Interval|Interval|Interval|Interval|Interval|Interval|Interval|Interval;
            I::months(14)
            I::days(12)
            I::minutes(12)
            I::hours(-86)
            I::minutes(86 * 60)
            I::hours(12)
            I::months(-12_011)
            I::months(30)
            I::hours(35)
            I::minutes(5)
            I::minutes(84 * 60 + 30)
            I::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400)
            I::seconds(-(12 * 3600 + 30 * 60 + 12))
            I::seconds(-(30 * 60 + 11))
            )),
        ),
        (
            "SELECT CAST('2021-08-25' AS DATE) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Date(NaiveDate::from_ymd(2021, 8, 25)))),
        ),
        (
            "SELECT CAST('08-25-2021' AS DATE) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Date(NaiveDate::from_ymd(2021, 8, 25)))),
        ),
        (
            r#"SELECT CAST('2021-08-025' AS DATE) FROM Item"#,
            Err(ValueError::LiteralCastToDateFailed("2021-08-025".to_string()).into()),
        ),
        (
            "SELECT CAST('AM 8:05' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms(8, 5, 0)))),
        ),
        (
            "SELECT CAST('AM 08:05' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms(8, 5, 0)))),
        ),
        (
            "SELECT CAST('AM 8:05:30' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms(8, 5, 30)))),
        ),
        (
            "SELECT CAST('AM 8:05:30.9' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms_milli(8, 5, 30, 900)))),
        ),
        (
            "SELECT CAST('8:05:30.9 AM' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms_milli(8, 5, 30, 900)))),
        ),
        (
            "SELECT CAST('25:08:05' AS TIME) AS cast FROM Item",
            Err(ValueError::LiteralCastToTimeFailed("25:08:05".to_string()).into()),
        ),
        (
            "SELECT CAST('2021-08-25 08:05:30' AS TIMESTAMP) AS cast FROM Item",
            Ok(
                select_with_null!(cast; Value::Timestamp(NaiveDate::from_ymd(2021, 8, 25).and_hms(8, 5, 30))),
            ),
        ),
        (
            "SELECT CAST('2021-08-25 08:05:30.9' AS TIMESTAMP) AS cast FROM Item",
            Ok(
                select_with_null!(cast; Value::Timestamp(NaiveDate::from_ymd(2021, 8, 25).and_hms_milli(8, 5, 30, 900))),
            ),
        ),
        (
            "SELECT CAST('2021-13-25 08:05:30' AS TIMESTAMP) AS cast FROM Item",
            Err(ValueError::LiteralCastToTimestampFailed("2021-13-25 08:05:30".to_string()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});

test_case!(cast_value, async move {
    // More test cases are in `gluesql::Value` unit tests.

    let test_cases = vec![
        (
            r#"
            CREATE TABLE Item (
                id INTEGER NULL,
                flag BOOLEAN,
                ratio FLOAT NULL,
                number TEXT
            )"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO Item VALUES (0, TRUE, NULL, "1")"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT CAST(LOWER(number) AS INTEGER) AS cast FROM Item"#,
            Ok(select!(cast I64; 1)),
        ),
        (
            r#"SELECT CAST(id AS BOOLEAN) AS cast FROM Item"#,
            Ok(select!(cast Bool; false)),
        ),
        (
            r#"SELECT CAST(flag AS TEXT) AS cast FROM Item"#,
            Ok(select!(cast Str; "TRUE".to_owned())),
        ),
        (
            r#"SELECT CAST(ratio AS INTEGER) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST(number AS BOOLEAN) FROM Item"#,
            Err(ValueError::ImpossibleCast.into()),
        ),
        (
            r#"
    CREATE TABLE IntervalLog (
        id INTEGER,
        interval_str_1 TEXT,
        interval_str_2 TEXT,
    )"#,
            Ok(Payload::Create),
        ),
        (
            r#"
    INSERT INTO IntervalLog VALUES
        (1, '"1-2" YEAR TO MONTH',         '"30" MONTH'),
        (2, '"12" DAY',                    '"35" HOUR'),
        (3, '"12" MINUTE',                 '"300" SECOND'),
        (4, '"-3 14" DAY TO HOUR',         '"3 12:30" DAY TO MINUTE'),
        (5, '"3 14:00:00" DAY TO SECOND',  '"3 12:30:12.1324" DAY TO SECOND'),
        (6, '"12:00" HOUR TO MINUTE',      '"-12:30:12" HOUR TO SECOND'),
        (7, '"-1000-11" YEAR TO MONTH',    '"-30:11" MINUTE TO SECOND');
    "#,
            Ok(Payload::Insert(7)),
        ),
        (
            r#"SELECT id, CAST(interval_str_1 as INTERVAL) as stoi_1, CAST(interval_str_2 as INTERVAL) as stoi_2 FROM IntervalLog;"#,
            Ok(select!(
            id  | stoi_1          | stoi_2
            I64 | Interval            | Interval;
            1     I::months(14)         I::months(30);
            2     I::days(12)           I::hours(35);
            3     I::minutes(12)        I::minutes(5);
            4     I::hours(-86)         I::minutes(84 * 60 + 30);
            5     I::minutes(86 * 60)   I::microseconds((((84 * 60) + 30) * 60 + 12) * 1_000_000 + 132_400);
            6     I::hours(12)          I::seconds(-(12 * 3600 + 30 * 60 + 12));
            7     I::months(-12_011)    I::seconds(-(30 * 60 + 11))
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
