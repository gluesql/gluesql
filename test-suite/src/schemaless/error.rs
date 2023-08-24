use {
    crate::*,
    gluesql_core::error::{EvaluateError, InsertError, ValueError},
    serde_json::json,
};

test_case!(error, {
    let g = get_tester!();

    g.run("CREATE TABLE Item").await;
    g.run(
        format!(
            "INSERT INTO Item VALUES ('{}');",
            json!({
                "id": 100,
                "name": "Test 001",
                "dex": 324,
                "rare": false,
                "obj": {
                    "cost": 3000
                }
            })
        )
        .as_str(),
    )
    .await;

    g.run("CREATE TABLE Player").await;
    g.run(
        format!(
            "INSERT INTO Player VALUES ('{}'), ('{}');",
            json!({ "id": 1001, "name": "Beam", "flag": 1 }),
            json!({ "id": 1002, "name": "Seo" }),
        )
        .as_str(),
    )
    .await;

    g.run("CREATE TABLE Food").await;
    g.run(
        format!(
            "INSERT INTO Food VALUES (SUBSTR(SUBSTR(' hi{}', 4), 1));",
            json!({ "id": 1, "name": "meat", "weight": 10 }),
        )
        .as_str(),
    )
    .await;

    g.test(
        r#"
            INSERT INTO Item
            VALUES (
                '{ "a": 10 }',
                '{ "b": true }'
            );
        "#,
        Err(InsertError::OnlySingleValueAcceptedForSchemalessRow.into()),
    )
    .await;
    g.test(
        "INSERT INTO Item SELECT id, name FROM Item LIMIT 1",
        Err(InsertError::OnlySingleValueAcceptedForSchemalessRow.into()),
    )
    .await;
    g.test(
        "INSERT INTO Item VALUES ('[1, 2, 3]');",
        Err(ValueError::JsonObjectTypeRequired.into()),
    )
    .await;
    g.test(
        "INSERT INTO Item VALUES (true);",
        Err(EvaluateError::TextLiteralRequired("Boolean(true)".to_owned()).into()),
    )
    .await;
    g.test(
        "INSERT INTO Item VALUES (CAST(1 AS INTEGER) + 4)",
        Err(EvaluateError::MapOrStringValueRequired("5".to_owned()).into()),
    )
    .await;
    g.test(
        "INSERT INTO Item SELECT id FROM Item LIMIT 1",
        Err(InsertError::MapTypeValueRequired("100".to_owned()).into()),
    )
    .await;
    g.test(
        "SELECT id FROM Item WHERE id IN (SELECT * FROM Item)",
        Err(EvaluateError::SchemalessProjectionForInSubQuery.into()),
    )
    .await;
    g.test(
        "SELECT id FROM Item WHERE id = (SELECT * FROM Item LIMIT 1)",
        Err(EvaluateError::SchemalessProjectionForSubQuery.into()),
    )
    .await;
});
