use {
    crate::*,
    gluesql_core::{
        data::ValueError,
        executor::{EvaluateError, InsertError},
    },
    serde_json::json,
};

test_case!(error, async move {
    run!("CREATE TABLE Item");
    run!(format!(
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
    .as_str());

    run!("CREATE TABLE Player");
    run!(format!(
        "INSERT INTO Player VALUES ('{}'), ('{}');",
        json!({ "id": 1001, "name": "Beam", "flag": 1 }),
        json!({ "id": 1002, "name": "Seo" }),
    )
    .as_str());

    test!(
        r#"
            INSERT INTO Item
            VALUES (
                '{ "a": 10 }',
                '{ "b": true }'
            );
        "#,
        Err(InsertError::OnlySingleValueAcceptedForSchemalessRow.into())
    );
    test!(
        "INSERT INTO Item SELECT id, name FROM Item LIMIT 1",
        Err(InsertError::OnlySingleValueAcceptedForSchemalessRow.into())
    );
    test!(
        "INSERT INTO Item VALUES ('[1, 2, 3]');",
        Err(ValueError::JsonObjectTypeRequired.into())
    );
    test!(
        "INSERT INTO Item VALUES (true);",
        Err(EvaluateError::TextLiteralRequired("Boolean(true)".to_owned()).into())
    );
    test!(
        "INSERT INTO Item VALUES (CAST(1 AS INTEGER) + 4)",
        Err(EvaluateError::MapOrStringValueRequired("5".to_owned()).into())
    );
    test!(
        "INSERT INTO Item SELECT id FROM Item LIMIT 1",
        Err(InsertError::MapTypeValueRequired("100".to_owned()).into())
    );
    test!(
        "SELECT id FROM Item WHERE id IN (SELECT * FROM Item)",
        Err(EvaluateError::SchemalessProjectionForInSubQuery.into())
    );
    test!(
        "SELECT id FROM Item WHERE id = (SELECT * FROM Item LIMIT 1)",
        Err(EvaluateError::SchemalessProjectionForSubQuery.into())
    );
});
