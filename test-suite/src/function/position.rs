use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(position, async move {
    let test_cases = [
        (r#"CREATE TABLE Food (name Text null)"#, Ok(Payload::Create)),
        (
            r#"INSERT INTO Food VALUES ("pork")"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"INSERT INTO Food VALUES ("burger")"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT POSITION("e" IN name) AS test FROM Food"#,
            Ok(select!(test; I64; 0; 5)),
        ),
        (
            r#"SELECT POSITION("s" IN "cheese") AS test"#,
            Ok(select!(test; I64; 5)),
        ),
        (
            r#"SELECT POSITION(1 IN "cheese") AS test"#,
            Err(EvaluateError::FunctionRequiresStringValue(String::from("POSITION")).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
