use {
    crate::*,
    gluesql_core::{
        executor::EvaluateError,
        prelude::{Payload, Value},
    },
};

test_case!(replace, async move {
    let test_cases = [
        (
            "CREATE TABLE Item (name TEXT DEFAULT REPLACE('SQL Tutorial', 'T', 'M'))",
            Ok(Payload::Create),
        ),
        (
            "INSERT INTO Item VALUES ('Tticky GlueTQL')",
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT REPLACE(name,'T','S') AS test FROM Item;",
            Ok(select!(
                "test"
                Value::Str;
                "Sticky GlueSQL".to_owned()
            )),
        )      ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
