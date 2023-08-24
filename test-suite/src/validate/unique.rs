use {
    crate::*,
    gluesql_core::{error::ValidateError, prelude::Value},
};

test_case!(unique, {
    let g = get_tester!();

    let queries = [
        r#"
        CREATE TABLE TestA (
            id INTEGER UNIQUE,
            num INT
        )
        "#,
        r#"
        CREATE TABLE TestB (
            id INTEGER UNIQUE,
            num INT UNIQUE
        )
        "#,
        r#"
        CREATE TABLE TestC (
            id INTEGER NULL UNIQUE,
            num INT
        )
        "#,
        "INSERT INTO TestA VALUES (1, 1)",
        "INSERT INTO TestA VALUES (2, 1), (3, 1)",
        "INSERT INTO TestB VALUES (1, 1)",
        "INSERT INTO TestB VALUES (2, 2), (3, 3)",
        "INSERT INTO TestC VALUES (NULL, 1)",
        "INSERT INTO TestC VALUES (2, 2), (NULL, 3)",
        "UPDATE TestC SET id = 1 WHERE num = 1",
        "UPDATE TestC SET id = NULL WHERE num = 1",
    ];

    for query in queries {
        g.run(query).await;
    }

    let error_cases = [
        (
            "INSERT INTO TestA VALUES (2, 2)",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "id".to_owned()).into(),
        ),
        (
            "INSERT INTO TestA VALUES (4, 4), (4, 5)",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(4), "id".to_owned()).into(),
        ),
        (
            "UPDATE TestA SET id = 2 WHERE id = 1",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "id".to_owned()).into(),
        ),
        (
            "INSERT INTO TestB VALUES (1, 3)",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(1), "id".to_owned()).into(),
        ),
        (
            "INSERT INTO TestB VALUES (4, 2)",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "num".to_owned()).into(),
        ),
        (
            "INSERT INTO TestB VALUES (5, 5), (6, 5)",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(5), "num".to_owned()).into(),
        ),
        (
            "UPDATE TestB SET num = 2 WHERE id = 1",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "num".to_owned()).into(),
        ),
        (
            "INSERT INTO TestC VALUES (2, 4)",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "id".to_owned()).into(),
        ),
        (
            "INSERT INTO TestC VALUES (NULL, 5), (3, 5), (3, 6)",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(3), "id".to_owned()).into(),
        ),
        (
            "UPDATE TestC SET id = 1",
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(1), "id".to_owned()).into(),
        ),
    ];

    for (sql, error) in error_cases {
        g.test(sql, Err(error)).await;
    }
});
