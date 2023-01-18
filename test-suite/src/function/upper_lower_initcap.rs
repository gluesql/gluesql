use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};

test_case!(upper_lower_initcap, async move {
    let test_cases = [
        (
            "CREATE TABLE Item (
                name TEXT DEFAULT UPPER('abc'),
                opt_name TEXT NULL DEFAULT LOWER('ABC'),
                opt_name_2 TEXT NULL DEFAULT INITCAP('a/b c')
            )",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES
            ('abcd', 'efgi', 'h/i jk'),
            ('Abcd', NULL, NULL),
            ('ABCD', 'EfGi', 'H/I JK')",
            Ok(Payload::Insert(3)),
        ),
        (
            "SELECT name FROM Item WHERE LOWER(name) = 'abcd';",
            Ok(select!(
                name Str;
                "abcd".to_owned();
                "Abcd".to_owned();
                "ABCD".to_owned()
            )),
        ),
        (
            "SELECT LOWER(name), UPPER(name), INITCAP(name) FROM Item;",
            Ok(select!(
                "LOWER(name)"      | "UPPER(name)"      | "INITCAP(name)"
                Str                | Str                | Str;
                "abcd".to_owned()    "ABCD".to_owned()    "Abcd".to_owned();
                "abcd".to_owned()    "ABCD".to_owned()    "Abcd".to_owned();
                "abcd".to_owned()    "ABCD".to_owned()    "Abcd".to_owned()
            )),
        ),
        (
            "
            SELECT
                LOWER('Abcd') as lower,
                UPPER('abCd') as upper,
                INITCAP('Abcd') as initcap
            FROM Item LIMIT 1;
            ",
            Ok(select!(
                lower             | upper             | initcap
                Str               | Str               | Str;
                "abcd".to_owned()   "ABCD".to_owned()   "Abcd".to_owned()
            )),
        ),
        (
            "SELECT LOWER(opt_name), UPPER(opt_name), INITCAP(opt_name) FROM Item;",
            Ok(select_with_null!(
                "LOWER(opt_name)"      | "UPPER(opt_name)"       | "INITCAP(opt_name)";
                Str("efgi".to_owned())   Str("EFGI".to_owned())    Str("Efgi".to_owned());
                Null                     Null                      Null;
                Str("efgi".to_owned())   Str("EFGI".to_owned())    Str("Efgi".to_owned())
            )),
        ),
        (
            "SELECT LOWER(opt_name_2), UPPER(opt_name_2), INITCAP(opt_name_2) FROM Item;",
            Ok(select_with_null!(
                "LOWER(opt_name_2)"      | "UPPER(opt_name_2)"       | "INITCAP(opt_name_2)";
                Str("h/i jk".to_owned())   Str("H/I JK".to_owned())    Str("H/I Jk".to_owned());
                Null                     Null                      Null;
                Str("h/i jk".to_owned())   Str("H/I JK".to_owned())    Str("H/I Jk".to_owned())
            )),
        ),
        (
            "SELECT LOWER() FROM Item",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "LOWER".to_owned(),
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
            Err(TranslateError::UnsupportedFunction("WHATEVER".to_owned()).into()),
        ),
        (
            "SELECT LOWER(a => 2) FROM Item",
            Err(TranslateError::NamedFunctionArgNotSupported.into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
