use {
    crate::*,
    gluesql_core::{
        error::{TranslateError, ValidateError},
        prelude::{Payload, Value},
    },
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
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(2), "id".to_owned())
                .into(),
        ),
        (
            "INSERT INTO TestA VALUES (4, 4), (4, 5)",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(4), "id".to_owned())
                .into(),
        ),
        (
            "UPDATE TestA SET id = 2 WHERE id = 1",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(2), "id".to_owned())
                .into(),
        ),
        (
            "INSERT INTO TestB VALUES (1, 3)",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(1), "id".to_owned())
                .into(),
        ),
        (
            "INSERT INTO TestB VALUES (4, 2)",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(2), "num".to_owned())
                .into(),
        ),
        (
            "INSERT INTO TestB VALUES (5, 5), (6, 5)",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(5), "num".to_owned())
                .into(),
        ),
        (
            "UPDATE TestB SET num = 2 WHERE id = 1",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(2), "num".to_owned())
                .into(),
        ),
        (
            "INSERT INTO TestC VALUES (2, 4)",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(2), "id".to_owned())
                .into(),
        ),
        (
            "INSERT INTO TestC VALUES (NULL, 5), (3, 5), (3, 6)",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(3), "id".to_owned())
                .into(),
        ),
        (
            "UPDATE TestC SET id = 1",
            ValidateError::duplicate_entry_on_single_unique_field(Value::I64(1), "id".to_owned())
                .into(),
        ),
    ];

    for (sql, error) in error_cases {
        g.test(sql, Err(error)).await;
    }
});

test_case!(unique_multi_key, {
    let g = get_tester!();

    let queries = [
        r#"
        CREATE TABLE TestA (
            id INTEGER,
            num INT,
            CONSTRAINT pk UNIQUE (id)
        )
        "#,
        r#"
        CREATE TABLE TestB (
            id INTEGER NULL,
            num INT,
            UNIQUE (id),
            UNIQUE (num)
        )
        "#,
        r#"
        CREATE TABLE TestC (
            id INTEGER NULL,
            num INT,
            CONSTRAINT pk UNIQUE (id)
        )
        "#,
        r#"
        CREATE TABLE TestD (
            id INTEGER NULL,
            num INT,
            CONSTRAINT pk UNIQUE (id, num)
        )
        "#,
        "INSERT INTO TestA VALUES (1, 1)",
        "INSERT INTO TestA VALUES (2, 1), (3, 1)",
        "INSERT INTO TestB VALUES (1, 1)",
        "INSERT INTO TestB VALUES (2, 2), (3, 3)",
        "INSERT INTO TestC VALUES (NULL, 1)",
        "INSERT INTO TestC VALUES (2, 2), (NULL, 3)",
        "INSERT INTO TestD VALUES (1, 1)",
        "INSERT INTO TestD VALUES (2, 2), (3, 3)",
        "INSERT INTO TestD VALUES (NULL, 4), (NULL, 4)",
        "UPDATE TestC SET id = 1 WHERE num = 1",
        "UPDATE TestC SET id = NULL WHERE num = 1",
    ];

    for query in queries {
        g.run(query).await;
    }

    let error_cases = [
        (
            "INSERT INTO TestA VALUES (2, 2)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(2)],
                vec!["id".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestA VALUES (4, 4), (4, 5)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(4)],
                vec!["id".to_owned()],
            )
            .into(),
        ),
        (
            "UPDATE TestA SET id = 2 WHERE id = 1",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(2)],
                vec!["id".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestB VALUES (1, 3)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(1)],
                vec!["id".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestB VALUES (4, 2)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(2)],
                vec!["num".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestB VALUES (5, 5), (6, 5)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(5)],
                vec!["num".to_owned()],
            )
            .into(),
        ),
        (
            "UPDATE TestB SET num = 2 WHERE id = 1",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(2)],
                vec!["num".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestC VALUES (2, 4)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(2)],
                vec!["id".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestC VALUES (NULL, 5), (3, 5), (3, 6)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(3)],
                vec!["id".to_owned()],
            )
            .into(),
        ),
        (
            "UPDATE TestC SET id = 1",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(1)],
                vec!["id".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestD VALUES (1, 1)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(1), Value::I64(1)],
                vec!["id".to_owned(), "num".to_owned()],
            )
            .into(),
        ),
        (
            "INSERT INTO TestD VALUES (2, 2), (3, 3)",
            ValidateError::duplicate_entry_on_multi_unique_field(
                vec![Value::I64(2), Value::I64(2)],
                vec!["id".to_owned(), "num".to_owned()],
            )
            .into(),
        ),
    ];

    for (sql, error) in error_cases {
        g.test(sql, Err(error)).await;
    }

    g.named_test(
        "Null insert should not trigger UNIQUE error.",
        r#"INSERT INTO TestD VALUES (NULL, 4), (NULL, 4)"#,
        Ok(Payload::Insert(2)),
    )
    .await;

    // We check that creating identical UNIQUE constraints at the column and later leads to an error
    g.named_test(
        "Creating identical UNIQUE constraints at the column and later leads to an error",
        r#"
        CREATE TABLE TestE (
            id INTEGER UNIQUE,
            num INT,
            UNIQUE (id)
        )
        "#,
        Err(TranslateError::DuplicatedUniqueConstraint("id".to_owned()).into()),
    )
    .await;

    // We check that creating identical UNIQUE constraints at the column and later leads to an error
    g.named_test(
        "Creating identical UNIQUE constraints at the column and later leads to an error",
        r#"
        CREATE TABLE TestF (
            id INTEGER,
            num INT UNIQUE,
            CONSTRAINT pk UNIQUE (num)
        )
        "#,
        Err(TranslateError::DuplicatedUniqueConstraint("num".to_owned()).into()),
    )
    .await;
});
