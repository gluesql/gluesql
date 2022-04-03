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
                1.abs()        -1::ceil(-1_f64)            i64::from(10).ceil()
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
                1.0_f64.ceil()  f64::abs(-1.0_f64)         f64::from(+1.0).abs()
            )),
        ),
        (
            "SELECT abs(0.0) AS ceil1, 
                    abs(-0.0) AS ceil2, 
                    abs(+0.0) AS ceil3 
            FROM SingleItem",

            Ok(select!(
                "abs1"        | "abs2"                   | "abs3";
                F64           | F64                      | F64;
                .00_f64.ceil()  f64::abs(-0.0_f64)         f64::from(+0.0).abs()
            )),
        ),

        (
            "SELECT abs(0) AS ceil1, 
                    abs(-0) AS ceil2, 
                    abs(+0) AS ceil3 
            FROM SingleItem",

            Ok(select!(
                "abs1"        | "abs2"                   | "abs3";
                i64           | i64                      | i64;
                0.ceil()        f64::abs(-0)               f64::from(+0).abs()
            )),
        ),

        (
            "SELECT abs('string') AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT abs(NULL) AS ceil FROM SingleItem",
            Ok(select_with_null!(abs; Null)),
        ),
        (
            "SELECT abs(TRUE) AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT abs(FALSE) AS ceil FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT abs('string', 'string2') AS ceil FROM SingleItem",
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
