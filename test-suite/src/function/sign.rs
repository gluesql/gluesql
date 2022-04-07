use crate::*;

test_case!(sign, async move {
    use gluesql_core::{
        executor::EvaluateError, executor::Payload, prelude::Value::*, translate::TranslateError,
    };
    let test_cases = vec![
        ("CREATE TABLE SingleItem (id INTEGER)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT SIGN(2) AS SIGN1, 
                    SIGN(-2) AS SIGN2, 
                    SIGN(+2) AS SIGN3 
            FROM SingleItem",
            Ok(select!(
                "SIGN1"        | "SIGN2"                   | "SIGN3";
                I8             | I8                        | I8;
                i8::from(1)             i8::from(-1)         i8::from(1)
            )),
        ),
        (
            "SELECT SIGN(2.0) AS SIGN1, 
                    SIGN(-2.0) AS SIGN2, 
                    SIGN(+2.0) AS SIGN3 
            FROM SingleItem",
            Ok(select!(
                "SIGN1"        | "SIGN2"                   | "SIGN3";
                I8           | I8                      | I8;
                i8::from(1)             i8::from(-1)         i8::from(1)
            )),
        ),
        (
            "SELECT SIGN(0.0) AS SIGN1, 
                    SIGN(-0.0) AS SIGN2, 
                    SIGN(+0.0) AS SIGN3 
            FROM SingleItem",
            Ok(select!(
                "SIGN1"        | "SIGN2"                   | "SIGN3";
                I8           | I8                      | I8;
                i8::from(0)             i8::from(0)         i8::from(0)
            )),
        ),
        (
            "SELECT SIGN(0) AS SIGN1, 
                    SIGN(-0) AS SIGN2, 
                    SIGN(+0) AS SIGN3 
            FROM SingleItem",
            Ok(select!(
                "SIGN1"        | "SIGN2"                   | "SIGN3";
                I8           | I8                      | I8;
                i8::from(0)             i8::from(0)         i8::from(0)
            )),
        ),
        (
            "SELECT SIGN('string') AS SIGN FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN(NULL) AS sign FROM SingleItem",
            Ok(select_with_null!(sign; Null)),
        ),
        (
            "SELECT SIGN(TRUE) AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN(FALSE) AS sign FROM SingleItem",
            Err(EvaluateError::FunctionRequiresFloatValue(String::from("SIGN")).into()),
        ),
        (
            "SELECT SIGN('string', 'string2') AS SIGN FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "SIGN".to_owned(),
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
