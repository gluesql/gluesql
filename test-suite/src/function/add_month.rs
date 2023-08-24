use gluesql_core::error::ValueError;

use {
    crate::*,
    chrono::format::ParseErrorKind,
    gluesql_core::{
        error::EvaluateError,
        prelude::{Error, Value},
    },
};

test_case!(add_month, {
    macro_rules! date {
        ($date: expr) => {
            $date.parse().unwrap()
        };
    }
    fn assert_chrono_error_kind_eq(error: Error, kind: ParseErrorKind) {
        match error {
            Error::Evaluate(EvaluateError::FormatParseError(err)) => {
                assert_eq!(err.kind(), kind)
            }
            _ => panic!("invalid error: {error}"),
        }
    }
    let g = get_tester!();

    g.named_test(
        "plus test on general case",
        "SELECT ADD_MONTH('2017-06-15',1) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2017-07-15")
        )),
    )
    .await;
    g.named_test(
        "minus test on general case",
        "SELECT ADD_MONTH('2017-06-15',-1) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2017-05-15")
        )),
    )
    .await;
    g.named_test(
        "the last day of February test",
        "SELECT ADD_MONTH('2017-01-31',1) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2017-02-28")
        )),
    )
    .await;
    g.named_test(
        "year change test",
        "SELECT ADD_MONTH('2017-01-31',13) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2018-02-28")
        )),
    )
    .await;
    g.named_test(
        "zero test",
        "SELECT ADD_MONTH('2017-01-31',0) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2017-01-31")
        )),
    )
    .await;
    g.named_test(
        "out of range test with i64::MAX",
        "SELECT ADD_MONTH('2017-01-31',9223372036854775807) AS test;",
        Err(ValueError::I64ToU32ConversionFailure("ADD_MONTH".to_owned()).into()),
    )
    .await;
    g.named_test(
        "out of range test",
        "SELECT ADD_MONTH('2017-01-31',10000000000000000000) AS test;",
        Err(EvaluateError::FunctionRequiresIntegerValue("ADD_MONTH".to_owned()).into()),
    )
    .await;
    g.named_test(
        "out of range test with i32::MAX",
        "SELECT ADD_MONTH('2017-01-31',2147483648) AS test;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    )
    .await;
    let error_cases = [
        (
            g.run_err("SELECT ADD_MONTH('2017-01-31-10',0) AS test;")
                .await,
            chrono::format::ParseErrorKind::TooLong,
        ),
        (
            g.run_err("SELECT ADD_MONTH('2017-01',0) AS test;").await,
            chrono::format::ParseErrorKind::TooShort,
        ),
        (
            g.run_err("SELECT ADD_MONTH('2015-14-05',1) AS test").await,
            chrono::format::ParseErrorKind::OutOfRange,
        ),
    ];

    for (error, kind) in error_cases {
        assert_chrono_error_kind_eq(error, kind);
    }
});
