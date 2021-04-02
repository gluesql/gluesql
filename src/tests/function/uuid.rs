use crate::*;

test_case!(uuid, async move {
    let test_cases = vec![
        (
            "CREATE TABLE test (id INT UNIQUE NOT NULL)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO test (id) VALUES (1)", Ok(Payload::Insert(1))),
        (
            "SELECT id FROM test WHERE UUID() IS NOT NULL", // Unreliable result
            Ok(select!(
                id
                Value::I64;
                1
            )),
        ),
        /*( Cannot test as INSERT is behaving poorly at the moment
            "INSERT INTO test (id) VALUES (UUID()), (UUID()), (UUID()), (UUID()), (UUID())",
            Ok(Payload::Insert(5)), // Should error if UUID isn't working
        ),
        (
            "INSERT INTO test (id) VALUES (UUID())",
            Ok(Payload::Insert(1)), // Should error if UUID isn't working
        ),*/

        /*( Cannot test as INSERT is behaving poorly at the moment
            "CREATE TABLE test_default (id INT UNIQUE NOT NULL DEFAULT UUID())",
            Ok(Payload::Create),
        ),
        ("INSERT INTO test_default (id) VALUES (NULL)", Ok(Payload::Insert(1))),
        (
            "SELECT id FROM test_default", // Unreliable result
            Ok(select!(
                id
                Value::I64;
                1
            )),
        ),*/
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
