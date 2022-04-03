use crate::*;

test_case!(abs, async move {
    use gluesql_core::{
        executor::EvaluateError, executor::Payload, prelude::Value::*, translate::TranslateError,
    };
    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id INTEGER)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT abs(1) AS abs1, 
                    abs(-1) AS abs2, 
                    abs(+1) AS abs3 
            FROM SingleItem",
            Ok(select!(
                "abs1"        | "abs2"                   | "abs3";
                I64           | I64                      | I64;
                1_i64.abs()        i64::abs(-1_i64)            i64::from(1).abs()
            )),
        ),
        (
            "SELECT abs(1.0) AS abs1, 
                    abs(-1.0) AS abs2, 
                    abs(+1.0) AS abs3 
            FROM SingleItem",

            Ok(select!(
                "abs1"        | "abs2"                   | "abs3";
                F64           | F64                      | F64;
                1.0_f64.abs()  f64::abs(-1.0_f64)         f64::from(1.0).abs()
            )),
        ),
        (
            "SELECT abs(0.0) AS abs1, 
                    abs(-0.0) AS abs2, 
                    abs(+0.0) AS abs3 
            FROM SingleItem",

            Ok(select!(
                "abs1"        | "abs2"                   | "abs3";
                F64           | F64                      | F64;
                0.0_f64.abs()  f64::abs(-0.0_f64)         f64::from(0.0).abs()
            )),
        ),

        (
            "SELECT abs(0) AS abs1, 
                    abs(-0) AS abs2, 
                    abs(+0) AS abs3 
            FROM SingleItem",

            Ok(select!(
                "abs1"        | "abs2"                   | "abs3";
                I64           | I64                      | I64;
                0_i64.abs()        i64::abs(-0)               i64::from(0).abs()
            )),
        ),

        (
            "SELECT abs('string') AS abs FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT abs(NULL) AS abs FROM SingleItem",
            Ok(select_with_null!(abs; Null)),
        ),
        (
            "SELECT abs(TRUE) AS abs FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT abs(FALSE) AS abs FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT abs('string', 'string2') AS abs FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "ABS".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
