use crate::*;

test_case!(insert, async move {
    let setup = vec![
        (
            "CREATE TABLE test (
                a INTEGER NULL,
                b TEXT NULL,
            )",
            Payload::Create,
        ),
        (
            "CREATE TABLE select_into (
                x INTEGER NULL,
                y TEXT NULL,
            )",
            Payload::Create,
        ),
        (
            "INSERT INTO select_into (x, y) VALUES (10, 'j')",
            Payload::Insert(1),
        ),
    ];
    for (sql, expected) in setup.into_iter() {
        test!(Ok(expected), sql);
    }
    let insert_cases = vec![
        "INSERT INTO test VALUES (1, 'a');",
        "INSERT INTO test (a, b) VALUES (2, 'b');",
        "INSERT INTO test (a) VALUES (3);",
        "INSERT INTO test (b) VALUES ('c');",
        "INSERT INTO test SELECT * FROM select_into;",
        "INSERT INTO test (a, b) SELECT * FROM select_into;",
        "INSERT INTO test SELECT x, y FROM select_into;",
        "INSERT INTO test (a, b) SELECT x, y FROM select_into;",
        "INSERT INTO test (a) SELECT x FROM select_into;",
        "INSERT INTO test (b) VALUES (UPPER('test'));",
        "INSERT INTO test (b) SELECT UPPER('test') FROM select_into;",
    ];

    for sql in insert_cases.into_iter() {
        test!(Ok(Payload::Insert(1)), sql);
    }

    let error_cases = vec![
        (
            "INSERT INTO test (a, b) VALUES (1, 'error', 'error')",
            RowError::WrongNumberOfValues,
        ),
        (
            "INSERT INTO test (a, b) VALUES (1, 'error'), (1, 'error', 'error')",
            RowError::WrongNumberOfValues,
        ),
    ];
    for (sql, expected) in error_cases.into_iter() {
        test!(Err(expected.into()), sql);
    }
});
