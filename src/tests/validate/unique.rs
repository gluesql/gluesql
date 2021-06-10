use crate::*;

test_case!(unique, async move {
    run!(
        r#"
CREATE TABLE TestA (
    id INTEGER UNIQUE,
    num INT
)"#
    );

    run!(
        r#"
CREATE TABLE TestB (
    id INTEGER UNIQUE,
    num INT UNIQUE
)"#
    );

    run!(
        r#"
CREATE TABLE TestC (
    id INTEGER NULL UNIQUE,
    num INT
)"#
    );

    run!("INSERT INTO TestA VALUES (1, 1)");
    run!("INSERT INTO TestA VALUES (2, 1), (3, 1)");

    run!("INSERT INTO TestB VALUES (1, 1)");
    run!("INSERT INTO TestB VALUES (2, 2), (3, 3)");

    run!("INSERT INTO TestC VALUES (NULL, 1)");
    run!("INSERT INTO TestC VALUES (2, 2), (NULL, 3)");
    run!("UPDATE TestC SET id = 1 WHERE num = 1");
    run!("UPDATE TestC SET id = NULL WHERE num = 1");

    let error_cases = vec![
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "id".to_owned()).into(),
            "INSERT INTO TestA VALUES (2, 2)",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(4), "id".to_owned()).into(),
            "INSERT INTO TestA VALUES (4, 4), (4, 5)",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "id".to_owned()).into(),
            "UPDATE TestA SET id = 2 WHERE id = 1",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(1), "id".to_owned()).into(),
            "INSERT INTO TestB VALUES (1, 3)",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "num".to_owned()).into(),
            "INSERT INTO TestB VALUES (4, 2)",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(5), "num".to_owned()).into(),
            "INSERT INTO TestB VALUES (5, 5), (6, 5)",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "num".to_owned()).into(),
            "UPDATE TestB SET num = 2 WHERE id = 1",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(2), "id".to_owned()).into(),
            "INSERT INTO TestC VALUES (2, 4)",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(3), "id".to_owned()).into(),
            "INSERT INTO TestC VALUES (NULL, 5), (3, 5), (3, 6)",
        ),
        (
            ValidateError::DuplicateEntryOnUniqueField(Value::I64(1), "id".to_owned()).into(),
            "UPDATE TestC SET id = 1",
        ),
    ];

    for (error, sql) in error_cases.into_iter() {
        test!(Err(error), sql);
    }
});
