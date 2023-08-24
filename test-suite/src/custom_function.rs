use {
    crate::*,
    gluesql_core::{
        error::{AlterError, EvaluateError, TranslateError},
        prelude::{Payload, PayloadVariable, Value::*},
    },
};

test_case!(custom, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE FUNCTION add_none()",
            Err(TranslateError::UnsupportedEmptyFunctionBody.into()),
        ),
        (
            "CREATE FUNCTION add_none() RETURN null",
            Ok(Payload::Create),
        ),
        (
            "CREATE FUNCTION add_zero(n INT) RETURN n",
            Ok(Payload::Create),
        ),
        (
            "CREATE FUNCTION add_zero(n INT) RETURN n",
            Err(AlterError::FunctionAlreadyExists("add_zero".to_owned()).into()),
        ),
        (
            "CREATE FUNCTION add_one (n INT, x INT DEFAULT 1) RETURN n + x",
            Ok(Payload::Create),
        ),
        (
            "CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT) RETURN n + x + y",
            Err(AlterError::NonDefaultArgumentFollowsDefaultArgument.into()),
        ),
        (
            "CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT DEFAULT 1) RETURN n + x + y",
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
            Ok(select_with_null!(
                r; Null
            )),
        ),
        (
            "SELECT add_two(1) as r",
            Ok(select!(
                r
                I64;
                3
            )),
        ),
        ("DROP FUNCTION add_none", Ok(Payload::DropFunction)),
        (
            "SHOW FUNCTIONS",
            Ok(Payload::ShowVariable(PayloadVariable::Functions(vec![
                "add_one(n: INT, x: INT)".to_owned(),
                "add_two(n: INT, x: INT, y: INT)".to_owned(),
                "add_zero(n: INT)".to_owned(),
            ]))),
        ),
        (
            "DROP FUNCTION add_none",
            Err(AlterError::FunctionNotFound("add_none".to_owned()).into()),
        ),
        (
            "DROP FUNCTION IF EXISTS add_zero, add_one, add_two",
            Ok(Payload::DropFunction),
        ),
        (
            "CREATE FUNCTION test(INT) RETURN 1",
            Err(TranslateError::UnNamedFunctionArgNotSupported.into()),
        ),
        (
            "CREATE FUNCTION test(a INT DEFAULT test()) RETURN 1",
            Err(EvaluateError::UnsupportedCustomFunction.into()),
        ),
        (
            "CREATE FUNCTION test(a INT, a BOOLEAN) RETURN 1",
            Err(AlterError::DuplicateArgName("a".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
