use {
    crate::*,
    gluesql_core::{
        data::ValueError,
        executor::EvaluateError,
        prelude::{Payload, PayloadVariable, Value::*},
        translate::TranslateError,
    },
};

test_case!(custom, async move {
    let test_cases = [
        ("CREATE FUNCTION add_none ()", Ok(Payload::Create)),
        (
            "CREATE FUNCTION add_one (n INT, x INT DEFAULT 1) RETURN n + x",
            Ok(Payload::Create),
        ),
        (
            "CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT) RETURN n + x + y",
            Ok(Payload::Create),
        ),
        ("SELECT add_none() AS r", Ok(select_with_null!(r; Null))),
        (
            "SELECT add_one(1) AS r",
            Ok(select!(
                r
                I64;
                2
            )),
        ),
        (
            "SELECT add_one(1, 8) AS r",
            Ok(select!(
                r
                I64;
                9
            )),
        ),
        (
            "SELECT add_one(1, 2, 4)",
            Err(EvaluateError::FunctionArgsLengthNotWithinRange {
                name: "add_one".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 3,
            }
            .into()),
        ),
        (
            "SELECT add_one()",
            Err(EvaluateError::FunctionArgsLengthNotWithinRange {
                name: "add_one".to_owned(),
                expected_minimum: 1,
                expected_maximum: 2,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT add_two(1, null, 2) as r",
            Ok(select!(
                r
                I64;
                4
            )),
        ),
        (
            "SELECT add_two(1, 2)",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
        ("DROP FUNCTION add_one, add_two", Ok(Payload::DropFunction)),
        (
            "SHOW FUNCTIONS",
            Ok(Payload::ShowVariable(PayloadVariable::Functions(vec![
                "add_none()".to_owned(),
            ]))),
        ),
        (
            "DROP FUNCTION IF EXISTS add_one, add_two, add_none",
            Ok(Payload::DropFunction),
        ),
        (
            "CREATE FUNCTION test(INT)",
            Err(TranslateError::UnNamedFunctionArgNotSupported.into()),
        ),
        (
            "CREATE TABLE test(a INT DEFAULT test())",
            Err(EvaluateError::UnsupportedCustomFunction.into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
