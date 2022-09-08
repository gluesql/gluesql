use crate::*;

test_case!(format, async move {
    use gluesql_core::{executor::EvaluateError, prelude::Value::*};

    let test_cases = vec![
        (
            r#"SELECT FORMAT(DATE "2017-06-15", "%Y-%m") AS DATE"#,
            Ok(select!(
                "DATE"
                Str;
                "2017-06".to_owned()
            )),
        ),
        (
            r#"SELECT FORMAT(TIMESTAMP "2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S") AS TIMESTAMP"#,
            Ok(select!(
                "TIMESTAMP"
                Str;
                "2015-09-05 23:56:04".to_owned()
            )),
        ),
        (
            r#"SELECT FORMAT("2015-09-05 23:56:04", "%Y-%m-%d %H") AS TIMESTAMP"#,
            Err(EvaluateError::FunctionRequiresFormattableValue("FORMAT".to_string()).into()),
        ),
        (
            r#"SELECT FORMAT(DATE "2017-06-15", "%9f") AS DATE"#,
            Err(EvaluateError::InvalidSpecifierGiven("%9f".to_string()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
