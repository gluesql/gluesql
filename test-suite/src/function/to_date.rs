test_case!(to_date, async move {
    use {gluesql_core::{executor::EvaluateError, prelude::Value::*},
    chrono::{ParseError}};

    let test_cases = vec![

        (
            r#"VALUES(TO_DATE("2017-06-15", "%Y-%m-%d"))"#,
            Ok(select!(
                column1
                Date;
                "2017-06-15".to_owned()
            )),
        ),
        (
            r#"VALUES(TO_TIMESTAMP("2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S"))"#,
            Ok(select!(
                column1
                Timestamp;
                "2015-09-05 23:56:04".to_owned()
            )),
        ),

        (
            r#"SELECT TO_DATE("2017-06-15","%Y-%m-%d") AS date"#,
            Ok(select!(
                date
                Date;
                "2017-06-15".to_owned()
            )),
        ),

        (
            r#"SELECT TO_TIMESTAMP("2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S") AS timestamp"#,
            Ok(select!(
                timestamp
                Timestamp;
                "2015-09-05 23:56:04".to_owned()
            )),
        ),
        (
            r#"SELECT TO_TIMESTAMP("2015-09-05 23:56:04", "%Y-%m-%d %H:%M") AS timestamp"#,
            Err(ParseError::TooLong),
        ),

        (
            r#"SELECT TO_TIMESTAMP("2015-09-05 23:56", "%Y-%m-%d %H:%M:%S") AS timestamp"#,
            Err(ParseError::TooShort),
        ),
            ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
