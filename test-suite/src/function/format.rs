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
            r#"SELECT 
         FORMAT(TIMESTAMP "2015-09-05 23:56:04", "%Y") AS YEAR
        ,FORMAT(TIMESTAMP "2015-09-05 23:56:04", "%m") AS MONTH
        ,FORMAT(TIMESTAMP "2015-09-05 23:56:04", "%d") AS DAY
        "#,
            Ok(select!(
            "YEAR" | "MONTH" | "DAY";
            Str | Str |Str;
            "2015".to_owned() "09".to_owned() "05".to_owned()
            )),
        ),
        (
            r#"SELECT FORMAT("2015-09-05 23:56:04", "%Y-%m-%d %H") AS TIMESTAMP"#,
            Err(
                EvaluateError::UnsupportedExprForFormatFunction("2015-09-05 23:56:04".to_string())
                    .into(),
            ),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
