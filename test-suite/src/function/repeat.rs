use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value},
        translate::TranslateError,
    },
};

test_case!(repeat, async move {
    let test_cases = [
        (
            "CREATE TABLE Item (name TEXT DEFAULT REPEAT('hello', 2))",
            Ok(Payload::Create),
        ),
        ("INSERT INTO Item VALUES ('hello')", Ok(Payload::Insert(1))),
        (
            "SELECT REPEAT(name, 2) AS test FROM Item",
            Ok(select!(
                "test"
                Value::Str;
                "hellohello".to_owned()
            )),
        ),
        (
            "SELECT REPEAT('abcd') AS test FROM Item",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "REPEAT".to_owned(),
                expected: 2,
                found: 1,
            }
            .into()),
        ),
        (
            "SELECT REPEAT('abcd', 2, 2) AS test FROM Item",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "REPEAT".to_owned(),
                expected: 2,
                found: 3,
            }
            .into()),
        ),
        (
            "SELECT REPEAT(1, 1) AS test FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("REPEAT".to_owned()).into()),
        ),
        (
            "SELECT REPEAT(name, null) AS test FROM Item",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "CREATE TABLE NullTest (name TEXT null)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO NullTest VALUES (null)", Ok(Payload::Insert(1))),
        (
            "SELECT REPEAT(name, 2) AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
