use crate::*;
use test::*;

test_case!(ltrim_rtrim, async move {
    use Value::Str;
    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES (" zzzytest"), ("testxxzx ")"#,
            Ok(Payload::Insert(2)),
        ),
        (
            r#"SELECT LTRIM(name) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "zzzytest".to_owned();
                "testxxzx ".to_owned()
            )),
        ),
        (
            r#"SELECT LTRIM(name, ' xyz') AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "test".to_owned();
                "testxxzx ".to_owned()
            )),
        ),
        (
            r#"SELECT RTRIM(name) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                " zzzytest".to_owned();
                "testxxzx".to_owned()
            )),
        ),
        (
            r#"SELECT RTRIM(name, 'xyz ') AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                " zzzytest".to_owned();
                "test".to_owned()
            )),
        ),
        (
            r#"SELECT LTRIM(1) AS test FROM Item"#,
            Err(EvaluateError::FunctionRequiresStringValue("LTRIM".to_owned()).into()),
        ),
        (
            r#"SELECT LTRIM(name, 1) AS test FROM Item"#,
            Err(EvaluateError::FunctionRequiresStringValue("LTRIM".to_owned()).into()),
        ),
        (
            r#"SELECT RTRIM(1) AS test FROM Item"#,
            Err(EvaluateError::FunctionRequiresStringValue("RTRIM".to_owned()).into()),
        ),
        (
            r#"SELECT RTRIM(name, 1) AS test FROM Item"#,
            Err(EvaluateError::FunctionRequiresStringValue("RTRIM".to_owned()).into()),
        ),
        (
            "CREATE TABLE NullTest (name TEXT null)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO NullTest VALUES (null)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT LTRIM(name) AS test FROM NullTest"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            r#"SELECT RTRIM(name) AS test FROM NullTest"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            r#"SELECT LTRIM(NULL, '123') AS test FROM NullTest"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            r#"SELECT LTRIM(name, NULL) AS test FROM NullTest"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            r#"SELECT RTRIM(NULL, '123') AS test FROM NullTest"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
        (
            r#"SELECT RTRIM(name, NULL) AS test FROM NullTest"#,
            Ok(select_with_null!(test; Value::Null)),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
