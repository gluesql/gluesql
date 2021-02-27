use crate::*;

test_case!(cast, async move {
    use Value::{Bool, Str, F64, I64};
    let test_cases = vec![
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES ("1"), ("2"), ("3")"#,
            Ok(Payload::Insert(3)),
        ),
        (
            r#"SELECT CAST("1" AS INTEGER) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast I64;
                1
            )),
        ),
        (
            r#"SELECT CAST("1.1" AS FLOAT) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast F64;
                1.1
            )),
        ),
        (
            r#"SELECT CAST(1 AS TEXT) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast Str;
                "1".to_string()
            )),
        ),
        (
            r#"SELECT CAST(1.1 AS INTEGER) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast I64;
                1
            )),
        ),
        (
            r#"SELECT CAST(1.1 AS TEXT) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast Str;
                "1.1".to_string()
            )),
        ),
        (
            r#"SELECT CAST(TRUE AS INTEGER) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast I64;
                1
            )),
        ),
        (
            r#"SELECT CAST(TRUE AS TEXT) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast Str;
                "TRUE".to_string()
            )),
        ),
        (
            r#"SELECT CAST(1 AS BOOLEAN) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast Bool;
                true
            )),
        ),
        (
            r#"SELECT CAST("TRUE" AS BOOLEAN) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast Bool;
                true
            )),
        ),
        (
            r#"SELECT CAST(LOWER(number) AS INTEGER) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast I64;
                1
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
            r#"SELECT CAST("BLEH" AS BOOLEAN) FROM Item LIMIT 1"#,
            Err(EvaluateError::ImpossibleCast.into()),
        ),
        /*( Known and ignored test case error
            r#"SELECT CAST(1 AS FLOAT) AS cast FROM Item LIMIT 1"#,
            Ok(select!(
                cast F64;
                1.0
            )),
        ),*/
    ];
    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
