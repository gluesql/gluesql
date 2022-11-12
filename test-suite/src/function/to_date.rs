use crate::*;
test_case!(to_date, async move {
    use {
        chrono::{NaiveDate, NaiveTime},
        gluesql_core::{
            executor::{ChronoFormatError, EvaluateError},
            prelude::Value::*,
        },
    };

    let test_cases = vec![
        (
            "VALUES(TO_DATE('2017-06-15', '%Y-%m-%d'))",
            Ok(select!(
                column1
                Date;
                NaiveDate::from_ymd_opt(2017, 6, 15).unwrap()
            )),
        ),
        (
            "VALUES(TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S'))",
            Ok(select!(
                column1
                Timestamp;
                NaiveDate::from_ymd_opt(2015, 9, 5).unwrap().and_hms_opt(23, 56, 4).unwrap()
            )),
        ),
        (
            "VALUES(TO_TIME('23:56:04', '%H:%M:%S'))",
            Ok(select!(
                column1
                Time;
                NaiveTime::from_hms_opt(23, 56, 4).unwrap()
            )),
        ),
        (
            "SELECT TO_DATE('2017-06-15','%Y-%m-%d') AS date",
            Ok(select!(
                date
                Date;
                NaiveDate::from_ymd_opt(2017, 6, 15).unwrap()
            )),
        ),
        (
            "SELECT TO_DATE('2017-jun-15','%Y-%b-%d') AS date",
            Ok(select!(
                date
                Date;
                NaiveDate::from_ymd_opt(2017, 6, 15).unwrap()
            )),
        ),
        (
            "SELECT TO_TIME('23:56:04','%H:%M:%S') AS time",
            Ok(select!(
                time
                Time;
                NaiveTime::from_hms_opt(23, 56, 4).unwrap()
            )),
        ),
        (
            "SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S') AS timestamp",
            Ok(select!(
                timestamp
                Timestamp;
                NaiveDate::from_ymd_opt(2015, 9, 5).unwrap().and_hms_opt(23, 56, 4).unwrap()
            )),
        ),
        (
            "SELECT TO_DATE('2015-09-05', '%Y-%m') AS date",
            Err(EvaluateError::ChronoFormat(ChronoFormatError::TooLong).into()),
        ),
        (
            "SELECT TO_TIME('23:56', '%H:%M:%S') AS time",
            Err(EvaluateError::ChronoFormat(ChronoFormatError::TooShort).into()),
        ),
        (
            "SELECT TO_TIMESTAMP('2015-05 23', '%Y-%d %H') AS timestamp",
            Err(EvaluateError::ChronoFormat(ChronoFormatError::NotEnough).into()),
        ),
        (
            "SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%S') AS timestamp;",
            Err(EvaluateError::ChronoFormat(ChronoFormatError::OutOfRange).into()),
        ),
        (
            "SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%%S') AS timestamp;",
            Err(EvaluateError::ChronoFormat(ChronoFormatError::Invalid).into()),
        ),
        (
            "SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%M') AS timestamp",
            Err(EvaluateError::ChronoFormat(ChronoFormatError::Impossible).into()),
        ),
        (
            "SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%') AS timestamp",
            Err(EvaluateError::ChronoFormat(ChronoFormatError::BadFormat).into()),
        ),
        (
            "SELECT TO_DATE(DATE '2017-06-15','%Y-%m-%d') AS date",
            Err(EvaluateError::FunctionRequiresStringValue("TO_DATE".to_owned()).into()),
        ),
        (
            "SELECT TO_TIMESTAMP(TIMESTAMP '2015-09-05 23:56:04','%Y-%m-%d') AS date",
            Err(EvaluateError::FunctionRequiresStringValue("TO_TIMESTAMP".to_owned()).into()),
        ),
        (
            "SELECT TO_TIME(TIME '23:56:04','%H:%M:%S') AS date",
            Err(EvaluateError::FunctionRequiresStringValue("TO_TIME".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
