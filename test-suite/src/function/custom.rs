use {
    crate::*,
    gluesql_core::{
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};

test_case!(custom, async move {
    let test_cases = [
        (
            "CREATE FUNCTION add_one (n INT, x INT DEFAULT 1)",
            Ok(Payload::Create),
        ),
        (
            "CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT)",
            Ok(Payload::Create),
        ),
        (
            "SELECT add_one(1)",
            Ok(select!(
                add_one
                I64;
                2
            )),
        ),
        (
            "SELECT add_one(1, 8)",
            Ok(select!(
                add_one
                I64;
                9
            )),
        ),
        (
            "SELECT add_one(1, 2, 4)",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "add_one".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 3,
            }
            .into()),
        ),
        (
            "SELECT add_one()",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "add_one".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT add_two(1, null, 2)",
            Ok(select!(
                add_one
                I64;
                3
            )),
        ),
        (
            "SELECT add_two(1, 2)",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "add_one".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 0,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
