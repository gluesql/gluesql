use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
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
            r#"SELECT POSITION("r" IN name) AS test FROM Food"#,
            Ok(select!(test; I64 | I64; 3 3)),
        ),
        (
            r#"SELECT POSITION("s" IN "cheese") AS test FROM Food"#,
            Ok(select!(test; I64; 5)),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
