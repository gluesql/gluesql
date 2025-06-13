use {
    crate::*,
    gluesql_core::{ast::DataType, error::TranslateError},
};

test_case!(generate_uuid, {
    let g = get_tester!();

    let test_cases = [(
        "SELECT generate_uuid(0) as uuid",
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "GENERATE_UUID".to_owned(),
            expected: 0,
            found: 1,
        }
        .into()),
    )];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }

    g.count("SELECT GENERATE_UUID()", 1).await;
    g.count("VALUES (GENERATE_UUID())", 1).await;
    g.type_match("SELECT GENERATE_UUID() as uuid", &[DataType::Uuid])
        .await;
    g.type_match("VALUES (GENERATE_UUID())", &[DataType::Uuid])
        .await;
});
