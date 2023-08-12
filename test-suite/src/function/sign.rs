use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(sign, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT
                SIGN(2) AS SIGN1, 
                SIGN(-2) AS SIGN2, 
                SIGN(+2) AS SIGN3 
            ;",
            Ok(select!(
                "SIGN1" | "SIGN2"                 | "SIGN3";
                I8      | I8                      | I8;
                1_i8      f64::signum(-2.0) as i8    1_i8
            )),
        ),
        (
            "SELECT
                SIGN(2.0) AS SIGN1, 
                SIGN(-2.0) AS SIGN2, 
                SIGN(+2.0) AS SIGN3 
            ;",
            Ok(select!(
                "SIGN1" | "SIGN2"                 | "SIGN3";
                I8      | I8                      | I8;
                1_i8      f64::signum(-2.0) as i8   1_i8
            )),
        ),
        (
            "SELECT
                SIGN(0.0) AS SIGN1, 
                SIGN(-0.0) AS SIGN2, 
                SIGN(+0.0) AS SIGN3 
            ;",
            Ok(select!(
                "SIGN1" | "SIGN2" | "SIGN3";
                I8      | I8      | I8;
                0_i8      0_i8      0_i8
            )),
        ),
        (
            "SELECT
                SIGN(0) AS SIGN1, 
                SIGN(-0) AS SIGN2, 
                SIGN(+0) AS SIGN3 
            ;",
            Ok(select!(
                "SIGN1" | "SIGN2" | "SIGN3";
                I8      | I8      | I8;
                0_i8      0_i8      0_i8
            )),
        ),
        (
            "SELECT SIGN('string') AS SIGN",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN(NULL) AS sign",
            Ok(select_with_null!(sign; Null)),
        ),
        (
            "SELECT SIGN(TRUE) AS sign",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN(FALSE) AS sign",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN('string', 'string2') AS SIGN",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "SIGN".to_owned(),
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
