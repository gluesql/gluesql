use crate::*;

test_case!(format, async move {
    use gluesql_core::{
        executor::{EvaluateError, SelectError},
        prelude::Value::*,
    };

    let test_cases = vec![
        (
            r#"SELECT FORMAT(DATE "2017-06-15", "%Y") AS DATE"#,
            Ok(select!(
                "DATE"
                Str;
                "2017".to_owned()
            )),
        ),
        (
            r#"SELECT FORMAT(DATE "2017-06-15", "%Y-%m") AS DATE"#,
            Ok(select!(
                "DATE"
                Str;
                "2017-06".to_owned()
            )),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
