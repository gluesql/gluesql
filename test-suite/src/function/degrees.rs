use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(degrees, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                DEGREES(PI() / 2) as degrees_1,
                DEGREES(PI()) as degrees_2
            ;",
            Ok(select!(
                degrees_1 | degrees_2;
                F64       | F64;
                90.0        180.0
            )),
        ),
        (
            "SELECT DEGREES(PI() / 2) as degrees_with_int;",
            Ok(select!(
                degrees_with_int
                F64;
                90.0
            )),
        ),
        (
            "SELECT DEGREES(0) as degrees_with_zero;",
            Ok(select!(
                degrees_with_zero
                F64;
                0.0
            )),
        ),
        (
            "SELECT DEGREES(RADIANS(90)) as radians_to_degrees;",
            Ok(select!(
                radians_to_degrees
                F64;
                90.0
            )),
        ),
        (
            "SELECT DEGREES(0, 0) as degrees_arg2;",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "DEGREES".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
        (
            "SELECT DEGREES() as degrees_arg0;",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "DEGREES".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT DEGREES('string') AS degrees;",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("DEGREES")).into()),
        ),
        (
            "SELECT DEGREES(NULL) AS degrees;",
            Ok(select_with_null!(degrees; Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
