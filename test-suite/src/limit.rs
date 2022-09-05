use {crate::*, gluesql_core::prelude::*, Value::*};

test_case!(limit, async move {
    let test_cases = [
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
        (
            "CREATE TABLE InsertTest (
                case_no INTEGER,
                id INTEGER
            )",
            Payload::Create,
        ),
        (
            "INSERT INTO InsertTest SELECT 1, id FROM Test OFFSET 1;",
            Payload::Insert(7),
        ),
        (
            "SELECT id FROM InsertTest WHERE case_no = 1",
            select!(id; I64; 2; 3; 4; 5; 6; 7; 8),
        ),
        (
            "INSERT INTO InsertTest SELECT 2, id FROM Test LIMIT 1;",
            Payload::Insert(1),
        ),
        (
            "SELECT id FROM InsertTest WHERE case_no = 2",
            select!(id; I64; 1),
        ),
        (
            "INSERT INTO InsertTest SELECT 3, id FROM Test ORDER BY id LIMIT 1 OFFSET 1;",
            Payload::Insert(1),
        ),
        (
            "SELECT id FROM InsertTest WHERE case_no = 3",
            select!(id; I64; 2),
        ),
        (
            "INSERT INTO InsertTest VALUES (4, 1), (4, 2), (4, 3), (4, 4) LIMIT 1;",
            Payload::Insert(1),
        ),
        (
            "SELECT id FROM InsertTest WHERE case_no = 4",
            select!(id; I64; 1),
        ),
        (
            "INSERT INTO InsertTest VALUES (5, 1), (5, 2), (5, 3), (5, 4) OFFSET 1;",
            Payload::Insert(3),
        ),
        (
            "SELECT id FROM InsertTest WHERE case_no = 5",
            select!(id; I64; 2; 3; 4),
        ),
        (
            "INSERT INTO InsertTest VALUES (6, 1), (6, 2), (6, 3), (6, 4) LIMIT 3 OFFSET 2;",
            Payload::Insert(2),
        ),
        (
            "SELECT id FROM InsertTest WHERE case_no = 6",
            select!(id; I64; 3; 4),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }
});
