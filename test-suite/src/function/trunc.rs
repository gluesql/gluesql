use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(trunc, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                TRUNC(0.3) AS trunc1,
                TRUNC(-0.8) AS trunc2,
                TRUNC(10) AS trunc3,
                TRUNC(6.87421) AS trunc4
            ;",
            Ok(select!(
                trunc1 | trunc2 | trunc3 | trunc4
                F64    | F64    | F64    | F64;
                0.0      0.0      10.0     6.0
            )),
        ),
        (
            "SELECT TRUNC('string') AS trunc",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("TRUNC")).into()),
        ),
        (
            "SELECT TRUNC(NULL) AS trunc",
            Ok(select_with_null!(trunc; Null)),
        ),
        (
            "SELECT TRUNC(TRUE) AS trunc",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("TRUNC")).into()),
        ),
        (
            "SELECT TRUNC(FALSE) AS trunc",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("TRUNC")).into()),
        ),
        (
            "SELECT TRUNC('string', 'string2') AS trunc",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "TRUNC".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
