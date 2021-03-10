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
            select_with_empty!(
                id     | num     | flag;
                I64(8)   I64(80)   Bool(true);
                I64(1)   I64(10)   Bool(false);
                I64(2)   I64(20)   Bool(false);
                I64(1)   I64(30)   Null;
                I64(1)   I64(40)   Bool(true)
            ),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }
});
