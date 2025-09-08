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
                Bytea;
                vec![0x47, 0x6C, 0x75, 0x65, 0x53, 0x51, 0x4C]
            )),
        ),
        (
            "SELECT UNHEX('48656C6C6F') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x48, 0x65, 0x6C, 0x6C, 0x6F]
            )),
        ),
        (
            "SELECT UNHEX('FF') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0xFF]
            )),
        ),
        (
            "SELECT UNHEX('FF00FF') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0xFF, 0x00, 0xFF]
            )),
        ),
        (
            "SELECT UNHEX('0x414243') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x41, 0x42, 0x43]
            )),
        ),
        (
            "SELECT UNHEX('0X414243') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x41, 0x42, 0x43]
            )),
        ),
        (
            "SELECT UNHEX('4a4B') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x4A, 0x4B]
            )),
        ),
        (
            "SELECT UNHEX('4a4B4c') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x4A, 0x4B, 0x4C]
            )),
        ),
        (
            "SELECT UNHEX('deadBEEF') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0xDE, 0xAD, 0xBE, 0xEF]
            )),
        ),
        (
            "SELECT UNHEX('A') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x0A]
            )),
        ),
        (
            "SELECT UNHEX('ABC') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x0A, 0xBC]
            )),
        ),
        (
            "SELECT UNHEX('0xA') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x0A]
            )),
        ),
        (
            "SELECT UNHEX('12345') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x01, 0x23, 0x45]
            )),
        ),
        (
            "SELECT UNHEX('00') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x00]
            )),
        ),
        (
            "SELECT UNHEX('000000') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![0x00, 0x00, 0x00]
            )),
        ),
        (
            "SELECT UNHEX('') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![]
            )),
        ),
        (
            "SELECT UNHEX('0x') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![]
            )),
        ),
        (
            "SELECT UNHEX('0X') AS unhex",
            Ok(select!(
                unhex
                Bytea;
                vec![]
            )),
        ),
        (
            "SELECT UNHEX(NULL) AS unhex",
            Ok(select_with_null!(unhex; Null)),
        ),
        (
            "SELECT UNHEX('GH') AS unhex",
            Err(EvaluateError::InvalidHexadecimal("GH".to_owned()).into()),
        ),
        (
            "SELECT UNHEX('INVALID') AS unhex",
            Err(EvaluateError::InvalidHexadecimal("INVALID".to_owned()).into()),
        ),
        (
            "SELECT UNHEX('41 42') AS unhex",
            Err(EvaluateError::InvalidHexadecimal("41 42".to_owned()).into()),
        ),
        (
            "SELECT UNHEX('41-42-43') AS unhex",
            Err(EvaluateError::InvalidHexadecimal("41-42-43".to_owned()).into()),
        ),
        (
            "SELECT UNHEX('0xGH') AS unhex",
            Err(EvaluateError::InvalidHexadecimal("0xGH".to_owned()).into()),
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
            "SELECT UNHEX('41', '42') AS unhex",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "UNHEX".to_owned(),
                expected: 1,
                found: 2,
            }
            .into()),
        ),
        (
            "SELECT UNHEX(123) AS unhex",
            Err(EvaluateError::FunctionRequiresStringValue("UNHEX".to_owned()).into()),
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
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
