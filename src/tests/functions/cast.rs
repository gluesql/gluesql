use crate::*;

test_case!(async move {
    use Value::*;

    let test_cases = vec![
        ("CREATE TABLE Item (number TEXT)", Ok(Payload::Create)),
        (r#"INSERT INTO Item VALUES ("1")"#, Ok(Payload::Insert(1))),
        (
            r#"SELECT CAST("TRUE" AS BOOLEAN) AS cast FROM Item"#,
            Ok(select!(cast Bool; true)),
        ),
        (
            r#"SELECT CAST(1 AS BOOLEAN) AS cast FROM Item"#,
            Ok(select!(cast Bool; true)),
        ),
        (
            r#"SELECT CAST("asdf" AS BOOLEAN) AS cast FROM Item"#,
            Err(ValueError::LiteralCastToBooleanFailed("asdf".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(3 AS BOOLEAN) AS cast FROM Item"#,
            Err(ValueError::LiteralCastToBooleanFailed("3".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(NULL AS BOOLEAN) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST("1" AS INTEGER) AS cast FROM Item"#,
            Ok(select!(cast I64; 1)),
        ),
        (
            r#"SELECT CAST("foo" AS INTEGER) AS cast FROM Item"#,
            Err(ValueError::LiteralCastFromTextToIntegerFailed("foo".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(1.1 AS INTEGER) AS cast FROM Item"#,
            Ok(select!(cast I64; 1)),
        ),
        (
            r#"SELECT CAST(TRUE AS INTEGER) AS cast FROM Item"#,
            Ok(select!(cast I64; 1)),
        ),
        (
            r#"SELECT CAST(NULL AS INTEGER) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST("1.1" AS FLOAT) AS cast FROM Item"#,
            Ok(select!(cast F64; 1.1)),
        ),
        (
            r#"SELECT CAST(1 AS FLOAT) AS cast FROM Item"#,
            Ok(select!(cast F64; 1.0)),
        ),
        (
            r#"SELECT CAST("foo" AS FLOAT) AS cast FROM Item"#,
            Err(ValueError::LiteralCastToFloatFailed("foo".to_owned()).into()),
        ),
        (
            r#"SELECT CAST(TRUE AS FLOAT) AS cast FROM Item"#,
            Ok(select!(cast F64; 1.0)),
        ),
        (
            r#"SELECT CAST(NULL AS FLOAT) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST(1 AS TEXT) AS cast FROM Item"#,
            Ok(select!(cast Str; "1".to_string())),
        ),
        (
            r#"SELECT CAST(1.1 AS TEXT) AS cast FROM Item"#,
            Ok(select!(cast Str; "1.1".to_string())),
        ),
        (
            r#"SELECT CAST(TRUE AS TEXT) AS cast FROM Item"#,
            Ok(select!(cast Str; "TRUE".to_string())),
        ),
        (
            r#"SELECT CAST(NULL AS TEXT) AS cast FROM Item"#,
            Ok(select_with_null!(cast; Null)),
        ),
        (
            r#"SELECT CAST(NULL AS NULL) FROM Item"#,
            Err(ValueError::UnimplementedLiteralCast {
                data_type: "NULL".to_owned(),
                literal: format!("{:?}", data::Literal::Null),
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});

mod cast_value {
    use crate::*;
    test_case!(async move {
        // More test cases are in `gluesql::Value` unit tests.

        use Value::*;

        let test_cases = vec![
            (
                r#"
                CREATE TABLE Item (
                    id INTEGER NULL,
                    flag BOOLEAN,
                    ratio FLOAT NULL,
                    number TEXT
                )"#,
                Ok(Payload::Create),
            ),
            (
                r#"INSERT INTO Item VALUES (0, TRUE, NULL, "1")"#,
                Ok(Payload::Insert(1)),
            ),
            (
                r#"SELECT CAST(LOWER(number) AS INTEGER) AS cast FROM Item"#,
                Ok(select!(cast I64; 1)),
            ),
            (
                r#"SELECT CAST(id AS BOOLEAN) AS cast FROM Item"#,
                Ok(select!(cast Bool; false)),
            ),
            (
                r#"SELECT CAST(flag AS TEXT) AS cast FROM Item"#,
                Ok(select!(cast Str; "TRUE".to_owned())),
            ),
            (
                r#"SELECT CAST(ratio AS INTEGER) AS cast FROM Item"#,
                Ok(select_with_null!(cast; Null)),
            ),
            (
                r#"SELECT CAST(number AS BOOLEAN) FROM Item"#,
                Err(ValueError::ImpossibleCast.into()),
            ),
            (
                r#"SELECT CAST(number AS NULL) FROM Item"#,
                Err(ValueError::UnimplementedCast.into()),
            ),
        ];

        for (sql, expected) in test_cases.into_iter() {
            test!(expected, sql);
        }
    });
}
