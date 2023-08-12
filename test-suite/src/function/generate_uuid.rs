use {
    crate::*,
    gluesql_core::{ast::DataType, error::TranslateError, prelude::Payload},
};

test_case!(generate_uuid, async move {
    let g = get_tester!();

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
        g.test(sql, expected).await;
    }

    g.count("SELECT GENERATE_UUID() FROM SingleItem", 1).await;
    g.type_match(
        "SELECT GENERATE_UUID() as uuid FROM SingleItem",
        &[DataType::Uuid],
    )
    .await;
});
