use {
    crate::*,
    gluesql_core::{
        error::EvaluateError,
        prelude::Value::*,
    },
};

test_case!(round, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                ROUND(0.3) AS round1,
                ROUND(-0.8) AS round2,
                ROUND(10) AS round3,
                ROUND(6.87421) AS round4
            ;",
            Ok(select!(
                round1 | round2          | round3 | round4
                F64    | F64             | F64    | F64;
                0.0      f64::from(-1)   10.0     7.0
            )),
        ),
        (
            "SELECT ROUND('string') AS round",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND(NULL) AS round",
            Ok(select_with_null!(round; Null)),
        ),
        (
            "SELECT ROUND(TRUE) AS round",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND(FALSE) AS round",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND('string', 'string2') AS round",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND(6.87421, 2) AS round",
            Ok(select!(round F64; 6.87)),
        ),
        (
            "SELECT ROUND(4321, -2) AS round",
            Ok(select!(round F64; 4300.0)),
        ),
        (
            "SELECT ROUND(1.23, 'precision') AS round",
            Err(EvaluateError::FunctionRequiresIntegerValue(String::from("ROUND")).into()),
        ),
        (
            "SELECT ROUND(1.23, 1.5) AS round",
            Err(EvaluateError::FunctionRequiresIntegerValue(String::from("ROUND")).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
