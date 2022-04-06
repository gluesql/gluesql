use crate::*;

test_case!(abs, async move {
    use gluesql_core::{
        executor::EvaluateError, executor::Payload, prelude::Value::*, translate::TranslateError,
    };
    let test_cases = vec![
        (
            "CREATE TABLE SingleItem (id integer, int8 int(8), dec decimal)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (0, -1, -2)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT ABS(1) AS ABS1, 
                    ABS(-1) AS ABS2, 
                    ABS(+1) AS ABS3 
            FROM SingleItem",
            Ok(select!(
                "ABS1"        | "ABS2"                   | "ABS3";
                I64           | I64                      | I64;
                1_i64.abs()        i64::abs(-1_i64)            i64::from(1).abs()
            )),
        ),
        (
            "SELECT ABS(1.0) AS ABS1, 
                    ABS(-1.0) AS ABS2, 
                    ABS(+1.0) AS ABS3 
            FROM SingleItem",

            Ok(select!(
                "ABS1"        | "ABS2"                   | "ABS3";
                F64           | F64                      | F64;
                1.0_f64.abs()  f64::abs(-1.0_f64)         f64::from(1.0).abs()
            )),
        ),
        (
            "SELECT ABS(0.0) AS ABS1, 
                    ABS(-0.0) AS ABS2, 
                    ABS(+0.0) AS ABS3 
            FROM SingleItem",

            Ok(select!(
                "ABS1"        | "ABS2"                   | "ABS3";
                F64           | F64                      | F64;
                0.0_f64.abs()  f64::abs(-0.0_f64)         f64::from(0.0).abs()
            )),
        ),

        (
            "SELECT ABS(0) AS ABS1, 
                    ABS(-0) AS ABS2, 
                    ABS(+0) AS ABS3 
            FROM SingleItem",

            Ok(select!(
                "ABS1"        | "ABS2"                   | "ABS3";
                I64           | I64                      | I64;
                0_i64.abs()        i64::abs(-0)               i64::from(0).abs()
            )),
        ),

        (
            "SELECT ABS(id) AS ABS1, 
                    ABS(int8) AS ABS2, 
                    ABS(dec) AS ABS3 
            FROM SingleItem",

            Ok(select!(
                "ABS1"        | "ABS2"                   | "ABS3";
                I64           | I8                      |  Decimal;
                0_i64.abs()        i8::abs(1)              2.into()
            )),
        ),

        (
            "SELECT ABS('string') AS ABS FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS(NULL) AS ABS FROM SingleItem",
            Ok(select_with_null!(ABS; Null)),
        ),
        (
            "SELECT ABS(TRUE) AS ABS FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS(FALSE) AS ABS FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("ABS")).into()),
        ),
        (
            "SELECT ABS('string', 'string2') AS ABS FROM SingleItem",
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
