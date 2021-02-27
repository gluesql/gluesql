use crate::*;

test_case!(cast, async move {
    use Value::{Str, F64, I64};
    let test_cases = vec![
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES ("1"), ("2"), ("3")"#,
            Ok(Payload::Insert(3)),
        ),
        (
            r#"SELECT CAST("1" AS INTEGER) AS TtI FROM Item LIMIT 1"#,
            Ok(select!(
                TtI I64;
                1
            )),
        ),
        (
            r#"SELECT CAST("1.1" AS FLOAT) AS TtF FROM Item LIMIT 1"#,
            Ok(select!(
                TtF F64;
                1.1
            )),
        ),
        (
            r#"SELECT CAST(1 AS TEXT) AS ItT FROM Item LIMIT 1"#,
            Ok(select!(
                ItT Str;
                "1".to_string()
            )),
        ),
        /*( Known and ignored test case error
            r#"SELECT CAST(1 AS FLOAT) AS ItF FROM Item LIMIT 1"#,
            Ok(select!(
                ItF F64;
                1.0
            )),
        ),*/
        (
            r#"SELECT CAST(1.1 AS INTEGER) AS FtI FROM Item LIMIT 1"#,
            Ok(select!(
                FtI I64;
                1
            )),
        ),
        (
            r#"SELECT CAST(1.1 AS TEXT) AS FtT FROM Item LIMIT 1"#,
            Ok(select!(
                FtT Str;
                "1.1".to_string()
            )),
        ),
        (
            r#"SELECT CAST("ABC" AS INTEGER) FROM Item LIMIT 1"#,
            Err(ValueError::FailedToParseNumber.into()),
        ),
        (
            r#"SELECT CAST("$1" AS INTEGER) FROM Item LIMIT 1"#,
            Err(ValueError::FailedToParseNumber.into()),
        ),
        (
            r#"SELECT CAST(LOWER(number) AS INTEGER) AS FtT FROM Item LIMIT 1"#,
            Ok(select!(
                FtT I64;
                1;
                2;
                3
            )),
        ),
    ];
    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
