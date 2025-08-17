use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(unhex, {
    let g = get_tester!();

    let test_cases = [
        (
            "SELECT UNHEX('476C756553514C') AS unhex",
            Ok(select!(
                unhex
                Str;
                "GlueSQL".to_string()
            )),
        ),
        (
            "SELECT UNHEX('FF') AS unhex",
            Ok(select!(
                unhex
                Str;
                {
                    let bytes = vec![0xFF];
                    String::from_utf8_lossy(&bytes).to_string()
                }
            )),
        ),
        (
            "SELECT UNHEX('48656C6C6F') AS unhex",
            Ok(select!(
                unhex
                Str;
                "Hello".to_string()
            )),
        ),
        (
            "SELECT UNHEX('0x414243') AS unhex",
            Ok(select!(
                unhex
                Str;
                "ABC".to_string()
            )),
        ),
        (
            "SELECT UNHEX('0X414243') AS unhex",
            Ok(select!(
                unhex
                Str;
                "ABC".to_string()
            )),
        ),
        (
            "SELECT UNHEX('4a4B') AS unhex",
            Ok(select!(
                unhex
                Str;
                "JK".to_string()
            )),
        ),
        (
            "SELECT UNHEX(NULL) AS unhex",
            Ok(select_with_null!(unhex; Null)),
        ),
        (
            "SELECT UNHEX('00') AS unhex",
            Ok(select!(
                unhex
                Str;
                "\0".to_string()
            )),
        ),
        (
            "SELECT UNHEX() AS unhex",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "UNHEX".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT UNHEX('A') AS unhex",
            Err(EvaluateError::InvalidHexadecimal("A".to_owned()).into()),
        ),
        (
            "SELECT UNHEX([1, 2, 3]) AS unhex",
            Err(EvaluateError::FunctionRequiresStringValue("UNHEX".to_owned()).into()),
        ),
        (
            "SELECT UNHEX(TRUE) AS unhex",
            Err(EvaluateError::FunctionRequiresStringValue("UNHEX".to_owned()).into()),
        ),
        (
            "SELECT UNHEX(FALSE) AS unhex",
            Err(EvaluateError::FunctionRequiresStringValue("UNHEX".to_owned()).into()),
        ),
        (
            "SELECT UNHEX('INVALID') AS unhex",
            Err(EvaluateError::InvalidHexadecimal("INVALID".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
