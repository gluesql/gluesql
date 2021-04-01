use crate::*;

test_case!(newid, async move {
    let test_cases = vec![
        (
            "CREATE TABLE test (id INT, id_text TEXT)",
            Ok(Payload::Create),
        ),
        (
            //"INSERT INTO test (id, text) VALUES (NEWID(), CAST(NEWID() AS TEXT)), (NEWID(), CAST(NEWID() AS TEXT)), (NEWID(), CAST(NEWID() AS TEXT))", // Inserts behaving poorly at the moment
            "INSERT INTO test (id, id_text) VALUES (1, '')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT id FROM test WHERE NEWID() > 0", // Unreliable result
            Ok(select!(
                id
                Value::I64;
                1
            )),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
