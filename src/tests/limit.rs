use crate::*;

test_case!(limit, async move {
    use Value::I64;

    let test_cases = vec![
        (
            "CREATE TABLE Test (
                id INTEGER
            )",
            Payload::Create,
        ),
        (
            "INSERT INTO Test VALUES (1), (2), (3), (4), (5), (6), (7), (8);",
            Payload::Insert(8),
        ),
        (
            "SELECT * FROM Test LIMIT 10;",
            select!(id; I64; 1; 2; 3; 4; 5; 6; 7; 8),
        ),
        (
            "SELECT * FROM Test LIMIT 10 OFFSET 1;",
            select!(id; I64; 2; 3; 4; 5; 6; 7; 8),
        ),
        (
            "SELECT * FROM Test OFFSET 2;",
            select!(id; I64; 3; 4; 5; 6; 7; 8),
        ),
        (
            "SELECT * FROM Test OFFSET 10;",
            Payload::Select {
                labels: vec!["id".to_owned()],
                rows: vec![],
            },
        ),
        (r#"SELECT * FROM Test LIMIT 3;"#, select!(id; I64; 1; 2; 3)),
        (
            r#"SELECT * FROM Test LIMIT 4 OFFSET 3;"#,
            select!(id; I64; 4; 5; 6; 7),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }
});
