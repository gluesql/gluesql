use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value},
    },
};

test_case!(trim, async move {
    let test_cases = [
        (
            "CREATE TABLE Item (
                name TEXT DEFAULT TRIM(LEADING 'a' FROM 'aabc') || TRIM('   good  ')
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES
                ('      Left blank'),
                ('Right blank     '),
                ('     Blank!     '),
                ('Not Blank');",
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
        (
            r#"SELECT TRIM(BOTH NULL FROM name) FROM NullName;"#,
            Ok(select_with_null!(
                "TRIM(BOTH NULL FROM name)";
                Value::Null
            )),
        ),
        (
            "SELECT TRIM(BOTH NULL FROM 'name') AS test",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "SELECT TRIM(TRAILING NULL FROM name) FROM NullName;",
            Ok(select_with_null!(
                "TRIM(TRAILING NULL FROM name)";
                Value::Null
            )),
        ),
        (
            "SELECT TRIM(LEADING NULL FROM name) FROM NullName;",
            Ok(select_with_null!(
                "TRIM(LEADING NULL FROM name)";
                Value::Null
            )),
        ),
        ("CREATE TABLE Test (name TEXT)", Ok(Payload::Create)),
        (
            "INSERT INTO Test VALUES
                    ('     blank     '), 
                    ('xxxyzblankxyzxx'), 
                    ('xxxyzblank     '),
                    ('     blankxyzxx'),
                    ('  xyzblankxyzxx'),
                    ('xxxyzblankxyz  ');",
            Ok(Payload::Insert(6)),
        ),
        (
            "SELECT TRIM(BOTH 'xyz' FROM name) FROM Test;",
            Ok(select!(
                "TRIM(BOTH 'xyz' FROM name)"
                Value::Str;
                "     blank     ".to_owned();
                "blank".to_owned();
                "blank     ".to_owned();
                "     blank".to_owned();
                "  xyzblank".to_owned();
                "blankxyz  ".to_owned()
            )),
        ),
        (
            "SELECT TRIM(LEADING 'xyz' FROM name) FROM Test;",
            Ok(select!(
                "TRIM(LEADING 'xyz' FROM name)"
                Value::Str;
                "     blank     ".to_owned();
                "blankxyzxx".to_owned();
                "blank     ".to_owned();
                "     blankxyzxx".to_owned();
                "  xyzblankxyzxx".to_owned();
                "blankxyz  ".to_owned()
            )),
        ),
        (
            r#"SELECT TRIM(TRAILING 'xyz' FROM name) FROM Test;"#,
            Ok(select!(
                "TRIM(TRAILING 'xyz' FROM name)"
                Value::Str;
                "     blank     ".to_owned();
                "xxxyzblank".to_owned();
                "xxxyzblank     ".to_owned();
                "     blank".to_owned();
                "  xyzblank".to_owned();
                "xxxyzblankxyz  ".to_owned()
            )),
        ),
        (
            "SELECT
                TRIM(BOTH '  hello  ') AS both,
                TRIM(LEADING '  hello  ') AS leading,
                TRIM(TRAILING '  hello  ') AS trailing
            ",
            Ok(select!(
                both               | leading              | trailing
                Value::Str         | Value::Str           | Value::Str;
                "hello".to_owned()   "hello  ".to_owned()   "  hello".to_owned()
            )),
        ),
        (
            "SELECT
                TRIM(BOTH TRIM(BOTH ' potato ')) AS Case1,
                TRIM('xyz' FROM 'x') AS Case2
            ",
            Ok(select!(
                Case1               | Case2
                Value::Str          | Value::Str;
                "potato".to_owned()   "".to_owned()
            )),
        ),
        (
            "SELECT TRIM('1' FROM 1) AS test FROM Test",
            Err(EvaluateError::FunctionRequiresStringValue("TRIM".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
