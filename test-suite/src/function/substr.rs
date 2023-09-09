use {
    crate::*,
    gluesql_core::{
        ast::BinaryOperator,
        error::EvaluateError,
        prelude::{Payload, Value::*},
    },
};

test_case!(substr, {
    let g = get_tester!();

    let test_cases = [
        (
            "CREATE TABLE Item (name TEXT DEFAULT SUBSTR('abc', 0, 2))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES ('Blop mc blee'), ('B'), ('Steven the &long named$ folken!')",
            Ok(Payload::Insert(3)),
        ),
        ("CREATE TABLE SingleItem (food TEXT)", Ok(Payload::Create)),
        (
            "INSERT INTO SingleItem VALUES (SUBSTR('LobSter',1))",
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
            r#"SELECT SUBSTR(SUBSTR(name, 1), 1) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "Blop mc blee".to_owned();
                "B".to_owned();
                "Steven the &long named$ folken!".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE name = SUBSTR('ABC', 2, 1)",
            Ok(select!(
                "name"
                Str;
                "B".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = 'B'",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned();
                "B".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE 'B' = SUBSTR(name, 1, 1)",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned();
                "B".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = UPPER('b')",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned();
                "B".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE SUBSTR(name, 1, 4) = SUBSTR('Blop', 1)",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > SUBSTR('Blop', 1)",
            Ok(select!(
                "name"
                Str;
                "Steven the &long named$ folken!".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > 'B'",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned();
                "Steven the &long named$ folken!".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE 'B' < SUBSTR(name, 1, 4)",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned();
                "Steven the &long named$ folken!".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > UPPER('b')",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned();
                "Steven the &long named$ folken!".to_owned()
            )),
        ),
        (
            "SELECT * FROM Item WHERE UPPER('b') < SUBSTR(name, 1, 4)",
            Ok(select!(
                "name"
                Str;
                "Blop mc blee".to_owned();
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
            "SELECT SUBSTR('ABC', 0, 3) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "AB".to_owned()
            )),
        ),
        (
            "SELECT SUBSTR('ABC', 1, 3) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "ABC".to_owned()
            )),
        ),
        (
            "SELECT SUBSTR('ABC', 1, 999) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "ABC".to_owned()
            )),
        ),
        (
            "SELECT SUBSTR('ABC', -1000, 1003) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "AB".to_owned()
            )),
        ),
        (
            "SELECT SUBSTR('ABC', -1, 3) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "A".to_owned()
            )),
        ),
        (
            "SELECT SUBSTR('ABC', -1, 4) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "AB".to_owned()
            )),
        ),
        (
            "SELECT SUBSTR(SUBSTR('ABC', 2, 3), 1, 1) AS test FROM SingleItem",
            Ok(select!(
                "test"
                Str;
                "B".to_owned()
            )),
        ),
        (
            "SELECT SUBSTR('ABC', -1, NULL) AS test FROM SingleItem",
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
            "SELECT * FROM SingleItem WHERE TRUE AND SUBSTR('wine',2,3)",
            Err(EvaluateError::BooleanTypeRequired("ine".to_owned()).into()),
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
        (
            r#"SELECT SUBSTR('123', 2, 3) - '3' AS test FROM SingleItem"#,
            Err(EvaluateError::UnsupportedBinaryOperation {
                left: "StrSlice { source: \"123\", range: 1..3 }".to_owned(),
                op: BinaryOperator::Minus,
                right: "Literal(Text(\"3\"))".to_owned(),
            }
            .into()),
        ),
        (
            r#"SELECT +SUBSTR('123', 2, 3) AS test FROM SingleItem"#,
            Err(EvaluateError::UnsupportedUnaryPlus("23".to_owned()).into()),
        ),
        (
            r#"SELECT -SUBSTR('123', 2, 3) AS test FROM SingleItem"#,
            Err(EvaluateError::UnsupportedUnaryMinus("23".to_owned()).into()),
        ),
        (
            r#"SELECT SUBSTR('123', 2, 3)! AS test FROM SingleItem"#,
            Err(EvaluateError::UnsupportedUnaryFactorial("23".to_owned()).into()),
        ),
        (
            r#"SELECT ~SUBSTR('123', 2, 3) AS test FROM SingleItem"#,
            Err(EvaluateError::IncompatibleUnaryBitwiseNotOperation("23".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
