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
            "SELECT TRIM(1) FROM Item;",
            Err(EvaluateError::FunctionRequiresStringValue("TRIM".to_owned()).into()),
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
        ("CREATE TABLE Test (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Test VALUES 
                    ("     blank     "), 
                    ("xxxyzblankxyzxx"), 
                    ("xxxyzblank     "),
                    ("     blankxyzxx"),
                    ("  xyzblankxyzxx"),
                    ("xxxyzblankxyz  ");"#,
            Ok(Payload::Insert(6)),
        ),
        (
            r#"SELECT TRIM(BOTH 'xyz' FROM name) FROM Test;"#,
            Ok(select!(
                "TRIM(BOTH 'xyz' FROM name)"
                Value::Str;
                "blank".to_owned();
                "blank".to_owned();
                "blank".to_owned();
                "blank".to_owned();
                "xyzblank".to_owned();
                "blankxyz".to_owned()
            )),
        ),
        (
            r#"SELECT TRIM(LEADING 'xyz' FROM name) FROM Test;"#,
            Ok(select!(
                "TRIM(LEADING 'xyz' FROM name)"
                Value::Str;
                "blank     ".to_owned();
                "blankxyzxx".to_owned();
                "blank     ".to_owned();
                "blankxyzxx".to_owned();
                "xyzblankxyzxx".to_owned();
                "blankxyz  ".to_owned()
            )),
        ),
        (
            r#"SELECT TRIM(TRAILING 'xyz' FROM name) FROM Test;"#,
            Ok(select!(
                "TRIM(TRAILING 'xyz' FROM name)"
                Value::Str;
                "     blank".to_owned();
                "xxxyzblank".to_owned();
                "xxxyzblank".to_owned();
                "     blank".to_owned();
                "  xyzblank".to_owned();
                "xxxyzblankxyz".to_owned()
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
