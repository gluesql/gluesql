use {
    crate::*,
    gluesql_core::prelude::{Payload, Value::*},
};

test_case!(get_ddl, async move {
    let test_cases = [
        ("CREATE TABLE Foo (no INT)", Ok(Payload::Create)),
        (
            "SELECT GET_DDL('TABLE', 'Foo')",
            Ok(select!(
                "GET_DDL('TABLE', 'Foo')"
                Str;
                "CREATE TABLE Foo (no INT)".to_owned()
            )),
        ),
        ("CREATE TABLE Bar (no INT PRIMARY KEY)", Ok(Payload::Create)),
        (
            "SELECT GET_DDL('TABLE', 'Bar')",
            Ok(select!(
                "GET_DDL('TABLE', 'Bar')"
                Str;
                "CREATE TABLE Bar (no INT PRIMARY KEY)".to_owned()
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
