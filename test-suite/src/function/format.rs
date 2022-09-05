use crate::*;

test_case!(format, async move {
    use gluesql_core::{executor::EvaluateError, prelude::Value::*};

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
            r#"SELECT FORMAT(TIMESTAMP "2015-09-05 23:56:04", "%Y-%m-%d %H") AS TIMESTAMP"#,
            Ok(select!(
                "TIMESTAMP"
                Str;
                "2015-09-05 23".to_owned()
            )),
        ),
        (
            r#"SELECT FORMAT("2015-09-05 23:56:04", "%Y-%m-%d %H") AS TIMESTAMP"#,
            Err(EvaluateError::FunctionRequiresFormattableValue("FORMAT".to_string()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
