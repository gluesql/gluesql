use crate::*;
test_case!(to_date, async move {
    use {
        chrono::{format::ParseErrorKind, NaiveDate, NaiveDateTime},
        gluesql_core::prelude::Value::*,
    };

    let test_cases = vec![
        (
            r#"VALUES(TO_DATE("2017-06-15", "%Y-%m-%d"))"#,
            Ok(select!(
                column1
                Date;
                NaiveDate::from_ymd(2017, 6, 15)
            )),
        ),
        (
            r#"VALUES(TO_TIMESTAMP("2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S"))"#,
            Ok(select!(
                column1
                Timestamp;
                NaiveDate::from_ymd(2015, 9, 5).and_hms(23, 56, 4)
            )),
        ),
        (
            r#"SELECT TO_DATE("2017-06-15","%Y-%m-%d") AS date"#,
            Ok(select!(
                date
                Date;
                NaiveDate::from_ymd(2017, 6, 15)
            )),
        ),
        (
            r#"SELECT TO_TIMESTAMP("2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S") AS timestamp"#,
            Ok(select!(
                timestamp
                Timestamp;
                NaiveDate::from_ymd(2015, 9, 5).and_hms(23, 56, 4)
            )),
        ),
        (
            r#"SELECT TO_TIMESTAMP("2015-09-05 23:56:04", "%Y-%m-%d %H:%M") AS timestamp"#,
            Err(ParseErrorKind::TooLong),
        ),
        (
            r#"SELECT TO_TIMESTAMP("2015-09-05 23:56", "%Y-%m-%d %H:%M:%S") AS timestamp"#,
            Err(ParseErrorKind::TooShort),
        ),
        (
            r#"SELECT TO_TIMESTAMP("2015-05 23:56", "%Y-%d %H:%M:%S") AS timestamp"#,
            Err(ParseErrorKind::NotEnough),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
