use {crate::*, gluesql_core::prelude::*};

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
        #[cfg(feature = "sorter")]
        (
            "SELECT * FROM Test ORDER BY id DESC LIMIT 3",
            select!(id; I64; 8; 7; 6),
        ),
        (
            "SELECT id, COUNT(*) as c FROM Test GROUP BY id LIMIT 3 OFFSET 2",
            select!(
                id  | c;
                I64 | I64;
                3     1;
                4     1;
                5     1
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }
});
