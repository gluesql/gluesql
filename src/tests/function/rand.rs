use crate::*;

test_case!(rand, async move {
    let test_cases = vec![
        (
            "CREATE TABLE test (id INT UNIQUE NOT NULL)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO test (id) VALUES (1)", Ok(Payload::Insert(1))),
        (
            "SELECT id FROM test WHERE RAND() IS NOT NULL", // Unreliable result
            Ok(select!(
                id
                Value::I64;
                1
            )),
        ),
        (
            "SELECT id FROM test WHERE RAND() > 0 AND RAND() < 1", // Unreliable result
            Ok(select!(
                id
                Value::I64;
                1
            )),
        ),
        /*( Cannot test, see #197
            "INSERT INTO test (id) VALUES (RAND()), (RAND()), (RAND()), (RAND()), (RAND())",
            Ok(Payload::Insert(5)), // Should error if UUID isn't working
        ),
        (
            "INSERT INTO test (id) VALUES (RAND())",
            Ok(Payload::Insert(1)), // Should error if UUID isn't working
        ),*/

        /*( Cannot test, see #197
            "CREATE TABLE test_default (id INT UNIQUE NOT NULL DEFAULT RAND())",
            Ok(Payload::Create),
        ),
        ("INSERT INTO test_default (id) VALUES (NULL)", Ok(Payload::Insert(1))),
        (
            "SELECT 1 FROM test_default WHERE id > 0 AND id < 1", // Unreliable result
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
