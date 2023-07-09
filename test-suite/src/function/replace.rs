use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value},
    },
};

test_case!(replace, async move {
    let test_cases = [
        (
            "CREATE TABLE Item (name TEXT DEFAULT REPLACE('SQL Tutorial', 'T', 'M'))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES ('Tticky GlueTQL')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT REPLACE(name,'T','S') AS test FROM Item;",
            Ok(select!(
                "test"
                Value::Str;
                "Sticky GlueSQL".to_owned()
            )),
        )
        (
            "SELECT REPLACE('GlueSQL') AS test FROM Item",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "REPLACE".to_owned(),
                expected: 3,
                found: 1,
            }
            .into()),
        ),
        (
            "SELECT REPLACE('GlueSQL','G) AS test FROM Item",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "REPLACE".to_owned(),
                expected: 3,
                found: 2,
            }
            .into()),
        ),
        (
            "SELECT REPLACE(1,1,1) AS test FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("REPLACE".to_owned()).into()),
        ),
        (
            "SELECT REPLACE(name, null,null) AS test FROM Item",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "CREATE TABLE NullTest (name TEXT null)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO NullTest VALUES (null)", Ok(Payload::Insert(1))),
        (
            "SELECT REPLACE(name, 'G','T') AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
