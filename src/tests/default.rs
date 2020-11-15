use crate::*;

test_case!(default, async move {
    use Value::*;

    let test_cases = vec![
        (
            "CREATE TABLE Test (
                id INTEGER DEFAULT 1,
                num INTEGER,
                flag BOOLEAN NULL DEFAULT false
            )",
            Payload::Create,
        ),
        ("INSERT INTO Test VALUES (8, 80, true);", Payload::Insert(1)),
        ("INSERT INTO Test (num) VALUES (10);", Payload::Insert(1)),
        (
            "INSERT INTO Test (num, id) VALUES (20, 2);",
            Payload::Insert(1),
        ),
        (
            "INSERT INTO Test (num, flag) VALUES (30, NULL), (40, true);",
            Payload::Insert(2),
        ),
        (
            "SELECT * FROM Test;",
            select!(
                id  | num | flag
                I64 | I64 | OptBool;
                8     80    Some(true);
                1     10    Some(false);
                2     20    Some(false);
                1     30    None;
                1     40    Some(true)
            ),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }
});
