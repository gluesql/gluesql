use crate::*;

test_case!(trim, async move {
    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES
                ("      Left blank"), 
                ("Right blank     "), 
                ("     Blank!     "), 
                ("Not Blank");"#,
            Ok(Payload::Insert(4)),
        ),
        (
            "SELECT TRIM(name) FROM Item;",
            Ok(select!(
                "TRIM(name)"
                Value::Str;
                "Left blank".to_owned();
                "Right blank".to_owned();
                "Blank!".to_owned();
                "Not Blank".to_owned()
            )),
        ),
        (
            "SELECT TRIM() FROM Item;",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "TRIM".to_owned(),
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT TRIM(1) FROM Item;",
            Err(EvaluateError::FunctionRequiresStringValue("TRIM".to_owned()).into()),
        ),
        (
            "SELECT TRIM(a => 2) FROM Item;",
            Err(TranslateError::NamedFunctionArgNotSupported.into()),
        ),
        (
            "CREATE TABLE NullName (name TEXT NULL)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO NullName VALUES (NULL)", Ok(Payload::Insert(1))),
        (
            "SELECT TRIM(name) AS test FROM NullName;",
            Ok(select_with_null!(test; Value::Null)),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
