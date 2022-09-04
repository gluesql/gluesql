use {
    crate::*,
    gluesql_core::{
        ast::DateTimeField,
        data::{
            value::{
                Value::{self, *},
                ValueError,
            },
            IntervalError, LiteralError,
        },
        prelude::Payload,
        translate::TranslateError,
    },
};

test_case!(extract, async move {
    let test_cases = [
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        (r#"INSERT INTO Item VALUES ("1")"#, Ok(Payload::Insert(1))),
        (
            r#"SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 13)),
        ),
        (
            r#"SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 2016)),
        ),
        (
            r#"SELECT EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 12)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 31)),
        ),
        (
            r#"SELECT EXTRACT(MINUTE FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 30)),
        ),
        (
            r#"SELECT EXTRACT(SECOND FROM TIMESTAMP '2016-12-31 13:30:15') as extract FROM Item"#,
            Ok(select!("extract" I64; 15)),
        ),
        (
            r#"SELECT EXTRACT(SECOND FROM TIME '17:12:28') as extract FROM Item"#,
            Ok(select!("extract" I64; 28)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM DATE '2021-10-06') as extract FROM Item"#,
            Ok(select!("extract" I64; 6)),
        ),
        (
            r#"SELECT EXTRACT(YEAR FROM INTERVAL "3" YEAR) as extract FROM Item"#,
            Ok(select!("extract" I64; 3)),
        ),
        (
            r#"SELECT EXTRACT(MONTH FROM INTERVAL "4" MONTH) as extract FROM Item"#,
            Ok(select!("extract" I64; 4)),
        ),
        (
            r#"SELECT EXTRACT(DAY FROM INTERVAL "5" DAY) as extract FROM Item"#,
            Ok(select!("extract" I64; 5)),
        ),
        (
            r#"SELECT EXTRACT(HOUR FROM INTERVAL "6" HOUR) as extract FROM Item"#,
            Ok(select!("extract" I64; 6)),
        ),
        (
            r#"SELECT EXTRACT(MINUTE FROM INTERVAL "7" MINUTE) as extract FROM Item"#,
            Ok(select!("extract" I64; 7)),
        ),
        (
            r#"SELECT EXTRACT(SECOND FROM INTERVAL "8" SECOND) as extract FROM Item"#,
            Ok(select!("extract" I64; 8)),
        ),
        (
            r#"SELECT EXTRACT(HOUR FROM number) as extract FROM Item"#,
            Err(ValueError::ExtractFormatNotMatched {
                value: Value::Str("1".to_owned()),
                field: DateTimeField::Hour,
            }
            .into()),
        ),
        (
            r#"SELECT EXTRACT(HOUR FROM INTERVAL "7" YEAR) as extract FROM Item"#,
            Err(IntervalError::FailedToExtract.into()),
        ),
        (
            r#"SELECT EXTRACT(HOUR FROM 100) FROM Item"#,
            Err(LiteralError::CannotExtract.into()),
        ),
        (
            r#"SELECT EXTRACT(microseconds FROM "2011-01-1") FROM Item;"#,
            Err(TranslateError::UnsupportedDateTimeField("MICROSECONDS".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
