use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::Value::*,
        translate::TranslateError,
    },
};

test_case!(degrees, async move {
    let test_cases = [
        (
            "SELECT
                DEGREES(1.5707963) as degrees_1,
                DEGREES(3.1415926) as degrees_2
            ;",
            Ok(select!(
                degrees_1                  | degrees_2;
                F64                        | F64;
                1.5707963_f64.to_degrees()   3.1415926_f64.to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(1.5707963) as degrees_with_int;",
            Ok(select!(
                degrees_with_int
                F64;
                1.5707963_f64.to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(0) as degrees_with_zero;",
            Ok(select!(
                degrees_with_zero
                F64;
                0.0_f64.to_degrees()
            )),
        ),
        (
            "SELECT DEGREES(RADIANS(90)) as radians_to_degrees;",
            Ok(select!(
                radians_to_degrees
                F64;
                f64::from(90).to_radians().to_degrees()
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
        test!(sql, expected);
    }
});
