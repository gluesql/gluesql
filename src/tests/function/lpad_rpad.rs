use crate::*;

test_case!(lpad_rpad, async move {
    use Value::Str;

    let test_cases = vec![
        (
            "CREATE TABLE Item (name TEXT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO Item VALUES ("hello")"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT LPAD(name, 10), RPAD(name, 10) FROM Item",
            Ok(select!(
                "LPAD(name, 10)"         | "RPAD(name, 10)"
                Str                      | Str;
                "     hello".to_owned()    "hello     ".to_owned()
            )),
        ),
        (
            "SELECT LPAD(name, 10, 'ab'), RPAD(name, 10, 'ab') FROM Item",
            Ok(select!(
                "LPAD(name, 10, 'ab')"   | "RPAD(name, 10, 'ab')"
                Str                      | Str;
                "ababahello".to_owned()    "helloababa".to_owned()
            )),
        ),
        (
            "SELECT LPAD(name, 3), RPAD(name, 3) FROM Item",
            Ok(select!(
                "LPAD(name, 3)"   | "RPAD(name, 3)"
                Str               | Str;
                "hel".to_owned()    "hel".to_owned()
            )),
        ),
        (
            "SELECT LPAD(name, 3, 'ab'), RPAD(name, 3, 'ab') FROM Item",
            Ok(select!(
                "LPAD(name, 3, 'ab')"   | "RPAD(name, 3, 'ab')"
                Str                     | Str;
                "hel".to_owned()          "hel".to_owned()
            )),
        ),
        (
            "SELECT LPAD(name) FROM Item",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "LPAD".to_string(),
                expected_minimum: 2,
                expected_maximum: 3,
                found: 1,
            }
            .into()),
        ),
        (
            "SELECT RPAD(name) FROM Item",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "RPAD".to_string(),
                expected_minimum: 2,
                expected_maximum: 3,
                found: 1,
            }
            .into()),
        ),
        (
            "SELECT LPAD(name, 10, 'ab', 'cd') FROM Item",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "LPAD".to_string(),
                expected_minimum: 2,
                expected_maximum: 3,
                found: 4,
            }
            .into()),
        ),
        (
            "SELECT RPAD(name, 10, 'ab', 'cd') FROM Item",
            Err(TranslateError::FunctionArgsLengthNotWithinRange {
                name: "RPAD".to_string(),
                expected_minimum: 2,
                expected_maximum: 3,
                found: 4,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});

