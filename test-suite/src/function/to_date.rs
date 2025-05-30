use {
    crate::*,
    chrono::{NaiveDate, NaiveTime, format::ParseErrorKind},
    gluesql_core::{
        error::EvaluateError,
        prelude::{Error, Value::*},
    },
};

test_case!(to_date, {
    let g = get_tester!();

    fn assert_chrono_error_kind_eq(error: Error, kind: ParseErrorKind) {
        match error {
            Error::Evaluate(EvaluateError::FormatParseError(err)) => {
                assert_eq!(err.kind(), kind)
            }
            _ => panic!("invalid error: {error}"),
        }
    }

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
        g.test(sql, expected).await;
    }

    let error_cases = [
        (
            g.run_err("SELECT TO_DATE('2015-09-05', '%Y-%m') AS date")
                .await,
            chrono::format::ParseErrorKind::TooLong,
        ),
        (
            g.run_err("SELECT TO_TIME('23:56', '%H:%M:%S') AS time")
                .await,
            chrono::format::ParseErrorKind::TooShort,
        ),
        (
            g.run_err("SELECT TO_TIMESTAMP('2015-05 23', '%Y-%d %H') AS timestamp")
                .await,
            chrono::format::ParseErrorKind::NotEnough,
        ),
        (
            g.run_err(
                "SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%S') AS timestamp",
            )
            .await,
            chrono::format::ParseErrorKind::OutOfRange,
        ),
        (
            g.run_err(
                "SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%S') AS timestamp",
            )
            .await,
            chrono::format::ParseErrorKind::OutOfRange,
        ),
        (
            g.run_err(
                "SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%%S') AS timestamp;",
            )
            .await,
            chrono::format::ParseErrorKind::OutOfRange,
        ),
        (
            g.run_err(
                "SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%M') AS timestamp",
            )
            .await,
            chrono::format::ParseErrorKind::Impossible,
        ),
        (
            g.run_err(
                "SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%') AS timestamp",
            )
            .await,
            chrono::format::ParseErrorKind::BadFormat,
        ),
    ];

    for (error, kind) in error_cases {
        assert_chrono_error_kind_eq(error, kind);
    }
});
