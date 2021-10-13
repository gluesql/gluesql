use crate::*;
use test::*;

test_case!(left_right, async move {
    use Value::{Null, Str};

    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
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
            "CREATE TABLE NullableName (name TEXT NULL)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO NullableName VALUES ('name')"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT LEFT(name, 3) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "Blo".to_owned();
                "B".to_owned();
                "Ste".to_owned()
            )),
        ),
        (
            r#"SELECT RIGHT(name, 10) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "op mc blee".to_owned();
                "B".to_owned();
                "d$ folken!".to_owned()
            )),
        ),
        // TODO Concatenation
        /*(
            r#"SELECT LEFT((name + 'bobbert'), 10) AS test FROM Item"#,
            Ok(select!(
                "test"
                OptStr;
                "Blop mc blee".to_owned();
                "Bbobbert".to_owned();
                "Steven the".to_owned()
            )),
        ),*/
        (
            r#"SELECT LEFT('blue', 10) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "blue".to_owned()
            )),
        ),
        (
            r#"SELECT LEFT("blunder", 3) AS test FROM SingleItem"#,
            Ok(select!(
                "test"
                Str;
                "blu".to_owned()
            )),
        ),
        (
            r#"SELECT LEFT(name, 3) AS test FROM NullName"#,
            Ok(select_with_null!(test; Null)),
        ),
        (
            r#"SELECT LEFT('Words', number) AS test FROM NullNumber"#,
            Ok(select_with_null!(test; Null)),
        ),
        (
            r#"SELECT LEFT(name, number) AS test FROM NullNumber INNER JOIN NullName ON 1 = 1"#,
            Ok(select_with_null!(test; Null)),
        ),
        (
            r#"SELECT LEFT(name, 1) AS test FROM NullableName"#,
            Ok(select!(
                "test"
                Str;
                "n".to_owned()
            )),
        ),
        (
            r#"SELECT RIGHT(name, 10, 10) AS test FROM SingleItem"#,
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RIGHT".to_owned(),
                expected: 2,
                found: 3,
            }
            .into()),
        ),
        (
            r#"SELECT RIGHT(name) AS test FROM SingleItem"#,
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RIGHT".to_owned(),
                expected: 2,
                found: 1,
            }
            .into()),
        ),
        (
            r#"SELECT RIGHT() AS test FROM SingleItem"#,
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RIGHT".to_owned(),
                expected: 2,
                found: 0,
            }
            .into()),
        ),
        (
            r#"SELECT RIGHT(1, 1) AS test FROM SingleItem"#,
            Err(EvaluateError::FunctionRequiresStringValue("RIGHT".to_string()).into()),
        ),
        (
            r#"SELECT RIGHT('Words', 1.1) AS test FROM SingleItem"#,
            Err(EvaluateError::FunctionRequiresIntegerValue("RIGHT".to_string()).into()),
        ),
        (
            r#"SELECT RIGHT('Words', -4) AS test FROM SingleItem"#,
            Err(EvaluateError::FunctionRequiresUSizeValue("RIGHT".to_string()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
