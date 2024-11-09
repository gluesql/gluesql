use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::{Payload, Value::*},
    },
};

test_case!(initcap, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE Item (
                name TEXT DEFAULT 'abcd'
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES
            ('h/i jk'),
            (NULL),
            ('H/I JK')",
            Ok(Payload::Insert(3)),
        ),
        (
            "SELECT name FROM Item WHERE INITCAP(name) = 'H/I Jk';",
            Ok(select!(
                name Str;
                "h/i jk".to_owned();
                "H/I JK".to_owned()
            )),
        ),
        (
            "SELECT INITCAP(name) FROM Item;",
            Ok(select_with_null!(
                "INITCAP(name)";
                Str("H/I Jk".to_owned());
                Null;
                Str("H/I Jk".to_owned())
            )),
        ),
        (
            "SELECT INITCAP() FROM Item",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "INITCAP".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT INITCAP(1) FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("INITCAP".to_owned()).into()),
        ),
        (
            "SELECT INITCAP(a => 2) FROM Item",
            Err(TranslateError::NamedFunctionArgNotSupported.into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
