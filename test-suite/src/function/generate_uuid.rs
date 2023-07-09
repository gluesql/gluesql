use {
    crate::*,
    gluesql_core::{ast::DataType, error::TranslateError, prelude::Payload},
};

test_case!(generate_uuid, async move {
    let test_cases = [
        (
            "CREATE TABLE SingleItem (id UUID DEFAULT GENERATE_UUID())",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO SingleItem VALUES (GENERATE_UUID())"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT generate_uuid(0) as uuid FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "GENERATE_UUID".to_owned(),
                expected: 0,
                found: 1,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }

    count!(1, "SELECT GENERATE_UUID() FROM SingleItem");
    type_match!(
        &[DataType::Uuid],
        "SELECT GENERATE_UUID() as uuid FROM SingleItem"
    );
});
