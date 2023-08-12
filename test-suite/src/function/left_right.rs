use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value::*},
    },
};

test_case!(left_right, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE Item (name TEXT DEFAULT LEFT('abc', 1))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES ('Blop mc blee'), ('B'), ('Steven the &long named$ folken!')",
            Ok(Payload::Insert(3)),
        ),
        ("CREATE TABLE SingleItem (id INTEGER)", Ok(Payload::Create)),
        ("INSERT INTO SingleItem VALUES (0)", Ok(Payload::Insert(1))),
        (
            "CREATE TABLE NullName (name TEXT NULL)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO NullName VALUES (NULL)", Ok(Payload::Insert(1))),
        (
            "CREATE TABLE NullNumber (number INTEGER NULL)",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO NullNumber VALUES (NULL)",
            Ok(Payload::Insert(1)),
        ),
        (
            "CREATE TABLE NullableName (name TEXT NULL)",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO NullableName VALUES ('name')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT LEFT(name, 3) AS test FROM Item",
            Ok(select!(
                "test"
                Str;
                "Blo".to_owned();
                "B".to_owned();
                "Ste".to_owned()
            )),
        ),
        (
            "SELECT RIGHT(name, 10) AS test FROM Item",
            Ok(select!(
                "test"
                Str;
                "op mc blee".to_owned();
                "B".to_owned();
                "d$ folken!".to_owned()
            )),
        ),
        (
            "SELECT LEFT((name || 'bobbert'), 10) AS test FROM Item",
            Ok(select!(
                "test"
                Str;
                "Blop mc bl".to_owned();
                "Bbobbert".to_owned();
                "Steven the".to_owned()
            )),
        ),
        (
            "SELECT LEFT('blue', 10) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "blue".to_owned()
            )),
        ),
        (
            "SELECT LEFT('blunder', 3) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "blu".to_owned()
            )),
        ),
        (
            "SELECT LEFT(name, 3) AS test FROM NullName",
            Ok(select_with_null!(test; Null)),
        ),
        (
            "SELECT LEFT('Words', number) AS test FROM NullNumber",
            Ok(select_with_null!(test; Null)),
        ),
        (
            "SELECT LEFT(name, number) AS test FROM NullNumber INNER JOIN NullName ON 1 = 1",
            Ok(select_with_null!(test; Null)),
        ),
        (
            "SELECT LEFT(name, 1) AS test FROM NullableName",
            Ok(select!(
                "test"
                Str;
                "n".to_owned()
            )),
        ),
        (
            "SELECT RIGHT(name, 10, 10) AS test FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RIGHT".to_owned(),
                expected: 2,
                found: 3,
            }
            .into()),
        ),
        (
            "SELECT RIGHT(name) AS test FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RIGHT".to_owned(),
                expected: 2,
                found: 1,
            }
            .into()),
        ),
        (
            "SELECT RIGHT() AS test FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RIGHT".to_owned(),
                expected: 2,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT RIGHT(1, 1) AS test FROM SingleItem",
            Err(EvaluateError::FunctionRequiresStringValue("RIGHT".to_owned()).into()),
        ),
        (
            "SELECT RIGHT('Words', 1.1) AS test FROM SingleItem",
            Err(EvaluateError::FunctionRequiresIntegerValue("RIGHT".to_owned()).into()),
        ),
        (
            "SELECT RIGHT('Words', -4) AS test FROM SingleItem",
            Err(EvaluateError::FunctionRequiresUSizeValue("RIGHT".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
