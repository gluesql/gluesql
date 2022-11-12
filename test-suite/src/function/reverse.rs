use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value},
    },
};

test_case!(reverse, async move {
    let test_cases = [
        (
            "CREATE TABLE Item (name TEXT DEFAULT REVERSE('world'))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES ('Let''s meet')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT REVERSE(name) AS test FROM Item;",
            Ok(select!(
                "test"
                Value::Str;
                "teem s'teL".to_owned()
            )),
        ),
        (
            "SELECT REVERSE(1) AS test FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("REVERSE".to_owned()).into()),
        ),
        (
            "CREATE TABLE NullTest (name TEXT null)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO NullTest VALUES (null)", Ok(Payload::Insert(1))),
        (
            "SELECT REVERSE(name) AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
