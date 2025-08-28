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
                TRUNC(-42.8) AS trunc1,
                TRUNC(42.8) AS trunc2
            ;",
            Ok(select!(
                "trunc1" | "trunc2";
                F64 | F64;
                -42.0 42.0
            )),
        ),
        (
            "SELECT
                TRUNC(-42) AS trunc1,
                TRUNC(42) AS trunc2
            ;",
            Ok(select!(
                "trunc1" | "trunc2";
                I64 | I64;
                -42 42
            )),
        ),
        (
            "SELECT TRUNC('string') AS trunc;",
            Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(String::from("TRUNC")).into()),
        ),
        (
            "SELECT TRUNC(TRUE) AS trunc;",
            Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(String::from("TRUNC")).into()),
        ),
        (
            "SELECT TRUNC(FALSE) AS trunc;",
            Err(EvaluateError::FunctionRequiresFloatOrIntegerValue(String::from("TRUNC")).into()),
        ),
        (
            "SELECT TRUNC(NULL) AS trunc;",
            Ok(select_with_null!(trunc; Null)),
        ),
        (
            "SELECT TRUNC('string', 'string2') AS trunc;",
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
