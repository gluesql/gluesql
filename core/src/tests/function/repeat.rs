use crate::*;

test_case!(repeat, async move {
    use {
        executor::EvaluateError,
        prelude::{Payload, Value},
        translate::TranslateError,
    };

    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES ("hello")"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT REPEAT(name, 2) AS test FROM Item",
            Ok(select!(
                "test"
                Value::Str;
                "hellohello".to_owned()
            )),
        ),
        (
            r#"SELECT REPEAT("abcd") AS test FROM Item"#,
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "REPEAT".to_owned(),
                expected: 2,
                found: 1,
            }
            .into()),
        ),
        (
            r#"SELECT REPEAT("abcd", 2, 2) AS test FROM Item"#,
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "REPEAT".to_owned(),
                expected: 2,
                found: 3,
            }
            .into()),
        ),
        (
            r#"SELECT REPEAT(1, 1) AS test FROM Item"#,
            Err(EvaluateError::FunctionRequiresStringValue("REPEAT".to_owned()).into()),
        ),
        (
            r#"SELECT REPEAT(name, null) AS test FROM Item"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "CREATE TABLE NullTest (name TEXT null)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO NullTest VALUES (null)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT REPEAT(name, 2) AS test FROM NullTest"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
