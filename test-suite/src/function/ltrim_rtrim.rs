use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{
            Payload,
            Value::{self, *},
        },
    },
};

test_case!(ltrim_rtrim, async move {
    let test_cases = [
        (
            "CREATE TABLE Item (name TEXT DEFAULT RTRIM(LTRIM('   abc   ')))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES (' zzzytest'), ('testxxzx ')",
            Ok(Payload::Insert(2)),
        ),
        (
            "SELECT LTRIM('x', 'xyz') AS test from Item;",
            Ok(select!(
                "test"
                Str;
                "".to_owned();
                "".to_owned()
            )),
        ),
        (
            "SELECT LTRIM('txu', 'xyz') AS test from Item;",
            Ok(select!(
                "test"
                Str;
                "txu".to_owned();
                "txu".to_owned()
            )),
        ),
        (
            "SELECT LTRIM(name) AS test FROM Item",
            Ok(select!(
                "test"
                Str;
                "zzzytest".to_owned();
                "testxxzx ".to_owned()
            )),
        ),
        (
            "SELECT LTRIM(RTRIM('GlueSQLABC', 'ABC')) AS test FROM Item;",
            Ok(select!(
                "test"
                Str;
                "GlueSQL".to_owned();
                "GlueSQL".to_owned()
            )),
        ),
        (
            "SELECT LTRIM(name, ' xyz') AS test FROM Item",
            Ok(select!(
                "test"
                Str;
                "test".to_owned();
                "testxxzx ".to_owned()
            )),
        ),
        (
            "SELECT RTRIM(name) AS test FROM Item",
            Ok(select!(
                "test"
                Str;
                " zzzytest".to_owned();
                "testxxzx".to_owned()
            )),
        ),
        (
            "SELECT RTRIM(name, 'xyz ') AS test FROM Item",
            Ok(select!(
                "test"
                Str;
                " zzzytest".to_owned();
                "test".to_owned()
            )),
        ),
        (
            "SELECT RTRIM('x', 'xyz') AS test from Item;",
            Ok(select!(
                "test"
                Str;
                "".to_owned();
                "".to_owned()
            )),
        ),
        (
            "SELECT RTRIM('tuv', 'xyz') AS test from Item;",
            Ok(select!(
                "test"
                Str;
                "tuv".to_owned();
                "tuv".to_owned()
            )),
        ),
        (
            "SELECT RTRIM('txu', 'xyz') AS test from Item;",
            Ok(select!(
                "test"
                Str;
                "txu".to_owned();
                "txu".to_owned()
            )),
        ),
        (
            "SELECT LTRIM(1) AS test FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("LTRIM".to_owned()).into()),
        ),
        (
            "SELECT LTRIM(name, 1) AS test FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("LTRIM".to_owned()).into()),
        ),
        (
            "SELECT RTRIM(1) AS test FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("RTRIM".to_owned()).into()),
        ),
        (
            "SELECT RTRIM(name, 1) AS test FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("RTRIM".to_owned()).into()),
        ),
        (
            "CREATE TABLE NullTest (name TEXT null)",
            Ok(Payload::Create),
        ),
        ("INSERT INTO NullTest VALUES (null)", Ok(Payload::Insert(1))),
        (
            "SELECT LTRIM(name, NULL) AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "SELECT LTRIM(name) AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "SELECT RTRIM(name) AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "SELECT LTRIM(NULL, '123') AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "SELECT LTRIM(name, NULL) AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "SELECT RTRIM(NULL, '123') AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            "SELECT RTRIM(name, NULL) AS test FROM NullTest",
            Ok(select_with_null!(test; Value::Null)),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
