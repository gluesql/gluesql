use {
    crate::*,
    gluesql_core::{
        data::ValueError,
        prelude::{
            Payload,
            Value::{self, *},
        },
    },
};

test_case!(position, async move {
    let test_cases = [
        ("CREATE TABLE Food (name Text null)", Ok(Payload::Create)),
        ("INSERT INTO Food VALUES ('pork')", Ok(Payload::Insert(1))),
        ("INSERT INTO Food VALUES ('burger')", Ok(Payload::Insert(1))),
        (
            "SELECT POSITION('e' IN name) AS test FROM Food",
            Ok(select!(test; I64; 0; 5)),
        ),
        (
            "SELECT POSITION('s' IN 'cheese') AS test",
            Ok(select!(test; I64; 5)),
        ),
        (
            "SELECT POSITION(NULL IN 'cheese') AS test",
            Ok(select_with_null!(test; Null)),
        ),
        (
            "SELECT POSITION(1 IN 'cheese') AS test",
            Err(ValueError::NonStringParameterInPosition {
                from: Value::Str("cheese".to_owned()),
                sub: Value::I64(1),
            }
            .into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
