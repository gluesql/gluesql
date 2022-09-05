use crate::*;

test_case!(format, async move {
    use gluesql_core::{
        executor::{EvaluateError, SelectError},
        prelude::Value::*,
    };

    let test_cases = vec![(
        r#"SELECT FORMAT(DATE "2017-06-15", "%Y-%m")"#,
        Ok(select!("2017-06")),
    )];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
