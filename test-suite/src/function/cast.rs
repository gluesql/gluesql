use {
    crate::*,
    chrono::{NaiveDate, NaiveTime},
    gluesql_core::{
        data::Interval as I,
        error::ValueError,
        prelude::{
            DataType, Payload,
            Value::{self, *},
        },
    },
    rust_decimal::Decimal,
};

test_case!(cast_literal, async move {
    let test_cases = [
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        ("INSERT INTO Item VALUES ('1')", Ok(Payload::Insert(1))),
        (
            "CREATE TABLE test (mytext Text, myint8 Int8, myint Int, myfloat Float, mydec Decimal, mybool Boolean, mydate Date)",
            Ok(Payload::Create),
        ),
        (
            "CREATE TABLE utest (mytext Text, myuint8 UINT8, myint Int, myfloat Float, mydec Decimal, mybool Boolean, mydate Date)",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO utest VALUES ('foobar', 2, 2, 2.0, 2.0, true, '2001-09-11')",
            Ok(Payload::Insert(1)),
        ),
        (
            "INSERT INTO test VALUES ('foobar', -2, 2, 2.0, 2.0, true, '2001-09-11')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT CAST('TRUE' AS BOOLEAN) AS cast FROM Item",
            Ok(select!(cast Bool; true)),
        ),
        (
            "SELECT CAST(1 AS BOOLEAN) AS cast FROM Item",
            Ok(select!(cast Bool; true)),
        ),
        (
            "SELECT CAST('asdf' AS BOOLEAN) AS cast FROM Item",
            Err(ValueError::LiteralCastToBooleanFailed("asdf".to_owned()).into()),
        ),
        (
            "SELECT CAST(3 AS BOOLEAN) AS cast FROM Item",
            Err(ValueError::LiteralCastToBooleanFailed("3".to_owned()).into()),
        ),
        (
            "SELECT CAST(NULL AS BOOLEAN) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST('1' AS INTEGER) AS cast FROM Item",
            Ok(select!(cast I64; 1)),
        ),
        (
            "SELECT CAST(SUBSTR('123', 2, 3) AS INTEGER) AS cast FROM Item",
            Ok(select!(cast I64; 23)),
        ),
        (
            "SELECT CAST('foo' AS INTEGER) AS cast FROM Item",
            Err(ValueError::LiteralCastFromTextToIntegerFailed("foo".to_owned()).into()),
        ),
        (
            "SELECT CAST(1.1 AS INTEGER) AS cast FROM Item",
            Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int, "1.1".to_owned()).into()),
        ),
        (
            "SELECT CAST(TRUE AS INTEGER) AS cast FROM Item",
            Ok(select!(cast I64; 1)),
        ),
        (
            "SELECT CAST(NULL AS INTEGER) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST(255 AS INT8) AS cast FROM Item",
            Err(ValueError::LiteralCastToInt8Failed("255".to_owned()).into()),
        ),
        (
            "SELECT CAST('foo' AS UINT8) AS cast FROM Item",
            Err(ValueError::LiteralCastFromTextToUnsignedInt8Failed("foo".to_owned()).into()),
        ),
        (
            "SELECT CAST(-1 AS UINT8) AS cast FROM Item",
            Err(ValueError::LiteralCastToUnsignedInt8Failed("-1".to_owned()).into()),
        ),
        (
            "SELECT CAST('foo' AS UINT16) AS cast FROM Item",
            Err(ValueError::LiteralCastFromTextToUint16Failed("foo".to_owned()).into()),
        ),
        (
            "SELECT CAST(-1 AS UINT16) AS cast FROM Item",
            Err(ValueError::LiteralCastToUint16Failed("-1".to_owned()).into()),
        ),
        (
            "SELECT CAST('1.1' AS FLOAT) AS cast FROM Item",
            Ok(select!(cast F64; 1.1)),
        ),
        (
            "SELECT CAST(1 AS FLOAT) AS cast FROM Item",
            Ok(select!(cast F64; 1.0)),
        ),
        (
            "SELECT CAST('foo' AS FLOAT) AS cast FROM Item",
            Err(ValueError::LiteralCastFromTextToFloatFailed("foo".to_owned()).into()),
        ),
        (
            "SELECT CAST(TRUE AS FLOAT) AS cast FROM Item",
            Ok(select!(cast F64; 1.0)),
        ),
        (
            "SELECT CAST(NULL AS FLOAT) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST(true AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(1,0))),
        ),
        (
            "SELECT CAST(false AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(0,0))),
        ),
        (
            "SELECT CAST(number AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(1,0))),
        ),
        (
            "SELECT CAST('1.1' AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(11,1))),
        ),
        (
            "SELECT CAST(1 AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(10, 1))),
        ),
        (
            "SELECT CAST(-1 AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(-10, 1))),
        ),
        (
            "SELECT CAST('foo' AS Decimal) AS cast FROM Item",
            Err(ValueError::LiteralCastFromTextToDecimalFailed("foo".to_owned()).into()),
        ),
        (
            "SELECT CAST(NULL AS Decimal) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST(true AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(1,0))),
        ),
        (
            "SELECT CAST(false AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(0,0))),
        ),
        (
            "SELECT CAST(number AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(1,0))),
        ),
        (
            "SELECT CAST('1.1' AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(11,1))),
        ),
        (
            "SELECT CAST(1 AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(10, 1))),
        ),
        (
            "SELECT CAST(-1 AS Decimal) AS cast FROM Item",
            Ok(select!(cast Decimal; Decimal::new(-10, 1))),
        ),
        (
            "SELECT CAST('foo' AS Decimal) AS cast FROM Item",
            Err(ValueError::LiteralCastFromTextToDecimalFailed("foo".to_owned()).into()),
        ),
        (
            "SELECT CAST(NULL AS Decimal) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST(mytext AS Decimal) AS cast FROM test",
            Err(ValueError::ImpossibleCast.into()),
        ),
        (
            "SELECT CAST(myint8 AS Decimal) AS cast FROM test",
            Ok(select!(cast Decimal; Decimal::new(-2,0))),
        ),
        (
            "SELECT CAST(myuint8 AS Decimal) AS cast FROM utest",
            Ok(select!(cast Decimal; Decimal::new(2,0))),
        ),
        (
            "SELECT CAST(myint AS Decimal) AS cast FROM test",
            Ok(select!(cast Decimal; Decimal::new(2,0))),
        ),
        (
            "SELECT CAST(myfloat AS Decimal) AS cast FROM test",
            Ok(select!(cast Decimal; Decimal::new(2,0))),
        ),
        (
            "SELECT CAST(mydec AS Decimal) AS cast FROM test",
            Ok(select!(cast Decimal; Decimal::new(2,0))),
        ),
        (
            "SELECT CAST(mybool AS Decimal) AS cast FROM test",
            Ok(select!(cast Decimal; Decimal::new(1,0))),
        ),

        (
            "SELECT CAST(not(mybool) AS Decimal) AS cast FROM test",
            Ok(select!(cast Decimal; Decimal::new(0,0))),
        ),

        (
            "SELECT CAST(mydate AS Decimal) AS cast FROM test",
            Err(ValueError::ImpossibleCast.into()),
        ),
        (
            "SELECT CAST(1 AS TEXT) AS cast FROM Item",
            Ok(select!(cast Str; "1".to_owned())),
        ),
        (
            "SELECT CAST(1.1 AS TEXT) AS cast FROM Item",
            Ok(select!(cast Str; "1.1".to_owned())),
        ),
        (
            "SELECT CAST(TRUE AS TEXT) AS cast FROM Item",
            Ok(select!(cast Str; "TRUE".to_owned())),
        ),
        (
            "SELECT CAST(NULL AS TEXT) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST(NULL AS INTERVAL) FROM Item",
            Err(ValueError::UnimplementedLiteralCast {
                data_type: gluesql_core::ast::DataType::Interval,
                literal: format!("{:?}", gluesql_core::data::Literal::Null),
            }
            .into()),
        ),
        (
            "SELECT
            CAST('''1-2'' YEAR TO MONTH' as INTERVAL) as stoi_1,
            CAST('''12'' DAY' as INTERVAL) as stoi_2,
            CAST('''12'' MINUTE' as INTERVAL) as stoi_3,
            CAST('''-3 14'' DAY TO HOUR' as INTERVAL) as stoi_4,
            CAST('''3 14:00:00'' DAY TO SECOND' as INTERVAL) as stoi_5,
            CAST('''12:00'' HOUR TO MINUTE' as INTERVAL) as stoi_6,
            CAST('''-1000-11'' YEAR TO MONTH' as INTERVAL) as stoi_7,
            CAST('''30'' MONTH' as INTERVAL) as stoi_8,
            CAST('''35'' HOUR' as INTERVAL) as stoi_9,
            CAST('''300'' SECOND' as INTERVAL) as stoi_10,
            CAST('''3 12:30'' DAY TO MINUTE' as INTERVAL) as stoi_11,
            CAST('''3 12:30:12.1324'' DAY TO SECOND' as INTERVAL) as stoi_12,
            CAST('''-12:30:12'' HOUR TO SECOND' as INTERVAL) as stoi_13,
            CAST('''-30:11'' MINUTE TO SECOND' as INTERVAL) as stoi_14
            FROM Item",
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
            Ok(select_with_null!(cast; Value::Date(NaiveDate::from_ymd_opt(2021, 8, 25).unwrap()))),
        ),
        (
            "SELECT CAST('08-25-2021' AS DATE) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Date(NaiveDate::from_ymd_opt(2021, 8, 25).unwrap()))),
        ),
        (
            "SELECT CAST('2021-08-025' AS DATE) FROM Item",
            Err(ValueError::LiteralCastToDateFailed("2021-08-025".to_owned()).into()),
        ),
        (
            "SELECT CAST('AM 8:05' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms_opt(8, 5, 0).unwrap()))),
        ),
        (
            "SELECT CAST('AM 08:05' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms_opt(8, 5, 0).unwrap()))),
        ),
        (
            "SELECT CAST('AM 8:05:30' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms_opt(8, 5, 30).unwrap()))),
        ),
        (
            "SELECT CAST('AM 8:05:30.9' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms_milli_opt(8, 5, 30, 900).unwrap()))),
        ),
        (
            "SELECT CAST('8:05:30.9 AM' AS TIME) AS cast FROM Item",
            Ok(select_with_null!(cast; Value::Time(NaiveTime::from_hms_milli_opt(8, 5, 30, 900).unwrap()))),
        ),
        (
            "SELECT CAST('25:08:05' AS TIME) AS cast FROM Item",
            Err(ValueError::LiteralCastToTimeFailed("25:08:05".to_owned()).into()),
        ),
        (
            "SELECT CAST('2021-08-25 08:05:30' AS TIMESTAMP) AS cast FROM Item",
            Ok(
                select_with_null!(cast; Value::Timestamp(NaiveDate::from_ymd_opt(2021, 8, 25).unwrap().and_hms_opt(8, 5, 30).unwrap())),
            ),
        ),
        (
            "SELECT CAST('2021-08-25 08:05:30.9' AS TIMESTAMP) AS cast FROM Item",
            Ok(
                select_with_null!(cast; Value::Timestamp(NaiveDate::from_ymd_opt(2021, 8, 25).unwrap().and_hms_milli_opt(8, 5, 30, 900).unwrap())),
            ),
        ),
        (
            "SELECT CAST('2021-13-25 08:05:30' AS TIMESTAMP) AS cast FROM Item",
            Err(ValueError::LiteralCastToTimestampFailed("2021-13-25 08:05:30".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});

test_case!(cast_value, async move {
    // More test cases are in `gluesql::Value` unit tests.

    let test_cases = [
        (
            "
            CREATE TABLE Item (
                id INTEGER NULL,
                flag BOOLEAN,
                ratio FLOAT NULL,
                number TEXT
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES (0, TRUE, NULL, '1')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT CAST(LOWER(number) AS INTEGER) AS cast FROM Item",
            Ok(select!(cast I64; 1)),
        ),
        (
            "SELECT CAST(id AS BOOLEAN) AS cast FROM Item",
            Ok(select!(cast Bool; false)),
        ),
        (
            "SELECT CAST(flag AS TEXT) AS cast FROM Item",
            Ok(select!(cast Str; "TRUE".to_owned())),
        ),
        (
            "SELECT CAST(ratio AS INTEGER) AS cast FROM Item",
            Ok(select_with_null!(cast; Null)),
        ),
        (
            "SELECT CAST(number AS BOOLEAN) FROM Item",
            Err(ValueError::ImpossibleCast.into()),
        ),
        (
            "
        CREATE TABLE IntervalLog (
        id INTEGER,
        interval_str_1 TEXT,
        interval_str_2 TEXT,
    )",
            Ok(Payload::Create),
        ),
        (
            "
        INSERT INTO IntervalLog VALUES
        (1, '''1-2'' YEAR TO MONTH',         '''30'' MONTH'),
        (2, '''12'' DAY',                    '''35'' HOUR'),
        (3, '''12'' MINUTE',                 '''300'' SECOND'),
        (4, '''-3 14'' DAY TO HOUR',         '''3 12:30'' DAY TO MINUTE'),
        (5, '''3 14:00:00'' DAY TO SECOND',  '''3 12:30:12.1324'' DAY TO SECOND'),
        (6, '''12:00'' HOUR TO MINUTE',      '''-12:30:12'' HOUR TO SECOND'),
        (7, '''-1000-11'' YEAR TO MONTH',    '''-30:11'' MINUTE TO SECOND');
    ",
            Ok(Payload::Insert(7)),
        ),
        (
            "SELECT id, CAST(interval_str_1 as INTERVAL) as stoi_1, CAST(interval_str_2 as INTERVAL) as stoi_2 FROM IntervalLog;",
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
        )
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
