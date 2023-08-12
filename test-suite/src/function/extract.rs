use {
    crate::*,
    gluesql_core::{
        ast::DateTimeField,
        data::value::Value::{self, *},
        error::{IntervalError, TranslateError, ValueError},
        prelude::Payload,
    },
};

test_case!(extract, {
    let g = get_tester!();

    let test_cases = [
        (
            r#"SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#,
            Ok(select!("extract" I64; 13)),
        ),
        (
            r#"SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#,
            Ok(select!("extract" I64; 2016)),
        ),
        (
            r#"SELECT EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#,
            Ok(select!("extract" I64; 12)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#,
            Ok(select!("extract" I64; 31)),
        ),
        (
            r#"SELECT EXTRACT(MINUTE FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#,
            Ok(select!("extract" I64; 30)),
        ),
        (
            r#"SELECT EXTRACT(SECOND FROM TIMESTAMP '2016-12-31 13:30:15') as extract"#,
            Ok(select!("extract" I64; 15)),
        ),
        (
            r#"SELECT EXTRACT(SECOND FROM TIME '17:12:28') as extract"#,
            Ok(select!("extract" I64; 28)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM DATE '2021-10-06') as extract"#,
            Ok(select!("extract" I64; 6)),
        ),
        (
            "SELECT EXTRACT(YEAR FROM INTERVAL '3' YEAR) as extract",
            Ok(select!("extract" I64; 3)),
        ),
        (
            "SELECT EXTRACT(MONTH FROM INTERVAL '4' MONTH) as extract",
            Ok(select!("extract" I64; 4)),
        ),
        (
            "SELECT EXTRACT(DAY FROM INTERVAL '5' DAY) as extract",
            Ok(select!("extract" I64; 5)),
        ),
        (
            "SELECT EXTRACT(HOUR FROM INTERVAL '6' HOUR) as extract",
            Ok(select!("extract" I64; 6)),
        ),
        (
            "SELECT EXTRACT(MINUTE FROM INTERVAL '7' MINUTE) as extract",
            Ok(select!("extract" I64; 7)),
        ),
        (
            "SELECT EXTRACT(SECOND FROM INTERVAL '8' SECOND) as extract",
            Ok(select!("extract" I64; 8)),
        ),
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        ("INSERT INTO Item VALUES ('1')", Ok(Payload::Insert(1))),
        (
            r#"SELECT EXTRACT(HOUR FROM number) as extract FROM Item"#,
            Err(ValueError::ExtractFormatNotMatched {
                value: Value::Str("1".to_owned()),
                field: DateTimeField::Hour,
            }
            .into()),
        ),
        (
            "SELECT EXTRACT(HOUR FROM INTERVAL '7' YEAR) as extract",
            Err(IntervalError::FailedToExtract.into()),
        ),
        (
            r#"SELECT EXTRACT(HOUR FROM 100)"#,
            Err(ValueError::ExtractFormatNotMatched {
                value: Value::I64(100),
                field: DateTimeField::Hour,
            }
            .into()),
        ),
        (
            "SELECT EXTRACT(microseconds FROM '2011-01-1');",
            Err(TranslateError::UnsupportedDateTimeField("MICROSECONDS".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
