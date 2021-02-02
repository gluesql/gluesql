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

    run!("INSERT INTO TestA VALUES (1, 1)");
    run!("INSERT INTO TestA VALUES (2, 1), (3, 1)");

    run!("INSERT INTO TestB VALUES (1, 1)");
    run!("INSERT INTO TestB VALUES (2, 2), (3, 3)");

    let error_cases = vec![
        (
            ValueError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(2)),
                "id".to_owned(),
            )
            .into(),
            "INSERT INTO TestA VALUES (2, 2)",
        ),
        (
            ValueError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(4)),
                "id".to_owned(),
            )
            .into(),
            "INSERT INTO TestA VALUES (4, 4), (4, 5)",
        ),
        (
            ValueError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(2)),
                "id".to_owned(),
            )
            .into(),
            "UPDATE TestA SET id = 2 WHERE id = 1",
        ),
        (
            ValueError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(1)),
                "id".to_owned(),
            )
            .into(),
            "INSERT INTO TestB VALUES (1, 3)",
        ),
        (
            ValueError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(2)),
                "num".to_owned(),
            )
            .into(),
            "INSERT INTO TestB VALUES (4, 2)",
        ),
        (
            ValueError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(5)),
                "num".to_owned(),
            )
            .into(),
            "INSERT INTO TestB VALUES (5, 5), (6, 5)",
        ),
        (
            ValueError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(2)),
                "num".to_owned(),
            )
            .into(),
            "UPDATE TestB SET num = 2 WHERE id = 1",
        ),
    ];

    for (error, sql) in error_cases.into_iter() {
        test!(Err(error), sql);
    }
});
