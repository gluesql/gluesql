use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(substr, async move {
    let test_cases = [
        (
            r#"CREATE TABLE Item (name TEXT DEFAULT SUBSTR("abc", 0, 2))"#,
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO Item VALUES ("Blop mc blee"), ("B"), ("Steven the &long named$ folken!")"#,
            Ok(Payload::Insert(3)),
        ),
        ("CREATE TABLE SingleItem (id INTEGER)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "CREATE TABLE NullName (name TEXT NULL)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO NullName VALUES (NULL)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "CREATE TABLE NullNumber (number INTEGER NULL)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO NullNumber VALUES (NULL)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT SUBSTR(name, 1) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "Blop mc blee".to_owned();
                "B".to_owned();
                "Steven the &long named$ folken!".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR(name, 2) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "lop mc blee".to_owned();
                "".to_owned();
                "teven the &long named$ folken!".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR(name, 999) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "".to_owned();
                "".to_owned();
                "".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR('ABC', -3, 0) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR("ABC", 0, 3) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "AB".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR("ABC", 1, 3) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "ABC".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR("ABC", 1, 999) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "ABC".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR("ABC", -1000, 1003) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "AB".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR("ABC", -1, 3) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "A".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR("ABC", -1, 4) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "AB".to_owned()
            )),
        ),
        (
            r#"SELECT SUBSTR("ABC", -1, NULL) AS test FROM SingleItem"#,
            Ok(select_with_null!(test; Null)),
        ),
        (
            r#"SELECT SUBSTR(name, 3) AS test FROM NullName"#,
            Ok(select_with_null!(test; Null)),
        ),
        (
            r#"SELECT SUBSTR('Words', number) AS test FROM NullNumber"#,
            Ok(select_with_null!(test; Null)),
        ),
        (
            r#"SELECT SUBSTR(1, 1) AS test FROM SingleItem"#,
            Err(EvaluateError::FunctionRequiresStringValue("SUBSTR".to_owned()).into()),
        ),
        (
            r#"SELECT SUBSTR('Words', 1.1) AS test FROM SingleItem"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("SUBSTR".to_owned()).into()),
        ),
        (
            r#"SELECT SUBSTR('Words', 1, -4) AS test FROM SingleItem"#,
            Err(EvaluateError::NegativeSubstrLenNotAllowed.into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
