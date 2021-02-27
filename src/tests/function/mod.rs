pub mod cast;

use crate::*;

test_case!(function, async move {
    use Value::Str;

    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES ("abcd"), ("Abcd"), ("ABCD")"#,
            Ok(Payload::Insert(3)),
        ),
        (
            r#"SELECT name FROM Item WHERE LOWER(name) = "abcd";"#,
            Ok(select!(
                name Str;
                "abcd".to_owned();
                "Abcd".to_owned();
                "ABCD".to_owned()
            )),
        ),
        (
            "SELECT LOWER(name), UPPER(name) FROM Item;",
            Ok(select!(
                "LOWER(name)"      | "UPPER(name)"
                Str                | Str;
                "abcd".to_owned()    "ABCD".to_owned();
                "abcd".to_owned()    "ABCD".to_owned();
                "abcd".to_owned()    "ABCD".to_owned()
            )),
        ),
        (
            r#"
            SELECT
                LOWER("Abcd") as lower,
                UPPER("abCd") as upper
            FROM Item LIMIT 1;
            "#,
            Ok(select!(
                lower             | upper
                Str               | Str;
                "abcd".to_owned()   "ABCD".to_owned()
            )),
        ),
        (
            "SELECT LOWER() FROM Item",
            Err(EvaluateError::NumberOfFunctionParamsNotMatching {
                expected: 1,
                found: 0,
            }
            .into()),
        ),
        (
            "SELECT LOWER(1) FROM Item",
            Err(EvaluateError::FunctionRequiresStringValue("LOWER".to_owned()).into()),
        ),
        (
            "SELECT WHATEVER(1) FROM Item",
            Err(EvaluateError::FunctionNotSupported("WHATEVER".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
