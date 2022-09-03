use crate::*;

test_case!(format, async move {
    use gluesql_core::{
        executor::{EvaluateError, SelectError},
        prelude::Value::*,
    };

    let test_cases = vec![(
        r#"SELECT FORMAT("2017-06-15", "%Y")"#,
        Ok(select!(
            date
            Str;
           "2017".to_owned()
        )),
    )];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
