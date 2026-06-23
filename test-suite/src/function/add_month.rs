use {
    crate::*,
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
    fn assert_format_parse_error(error: &Error) {
        assert!(
            matches!(error, Error::Evaluate(EvaluateError::FormatParseError(_))),
            "expected FormatParseError, got: {error}"
        );
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
    );
    g.named_test(
        "minus test on general case",
        "SELECT ADD_MONTH('2017-06-15',-1) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2017-05-15")
        )),
    );
    g.named_test(
        "the last day of February test",
        "SELECT ADD_MONTH('2017-01-31',1) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2017-02-28")
        )),
    );
    g.named_test(
        "year change test",
        "SELECT ADD_MONTH('2017-01-31',13) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2018-02-28")
        )),
    );
    g.named_test(
        "zero test",
        "SELECT ADD_MONTH('2017-01-31',0) AS test;",
        Ok(select!(
            "test"
            Value::Date;
            date!("2017-01-31")
        )),
    );
    g.named_test(
        "out of range test with i64::MAX",
        "SELECT ADD_MONTH('2017-01-31',9223372036854775807) AS test;",
        Err(EvaluateError::I64ToU32ConversionFailure("ADD_MONTH".to_owned()).into()),
    );
    g.named_test(
        "out of range test",
        "SELECT ADD_MONTH('2017-01-31',10000000000000000000) AS test;",
        Err(EvaluateError::FunctionRequiresIntegerValue("ADD_MONTH".to_owned()).into()),
    );
    g.named_test(
        "out of range test with i32::MAX",
        "SELECT ADD_MONTH('2017-01-31',2147483648) AS test;",
        Err(EvaluateError::ChrFunctionRequiresIntegerValueInRange0To255.into()),
    );
    let error_cases = [
        g.run_err("SELECT ADD_MONTH('2017-01-31-10',0) AS test;"),
        g.run_err("SELECT ADD_MONTH('2017-01',0) AS test;"),
        g.run_err("SELECT ADD_MONTH('2015-14-05',1) AS test"),
    ];

    for error in &error_cases {
        assert_format_parse_error(error);
    }
});
