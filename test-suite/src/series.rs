use {
    crate::*,
    gluesql_core::{
        executor::FetchError,
        prelude::{Payload, Value::*},
        translate::TranslateError,
    },
};

test_case!(series, async move {
    let test_cases = vec![
        (
            // SERIES(N) has intenal column `N`
            "SELECT * FROM SERIES(3)",
            Ok(select!(
                N
                I64;
                1;
                2;
                3
            )),
        ),
        (
            // SERIES(N) with lowercase works
            "SELECT * FROM sErIeS(3)",
            Ok(select!(
                N
                I64;
                1;
                2;
                3
            )),
        ),
        (
            // SERIES(N) with table alias
            "SELECT S.* FROM SERIES(3) as S",
            Ok(select!(
                N
                I64;
                1;
                2;
                3
            )),
        ),
        (
            // SERIES with unary plus is allowed
            "SELECT * FROM SERIES(+3)",
            Ok(select!(
                N
                I64;
                1;
                2;
                3
            )),
        ),
        (
            // CTAS with SERIES(N)
            "CREATE TABLE SeriesTable AS SELECT * FROM SERIES(3)",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM SeriesTable",
            Ok(select!(
                N
                I64;
                1;
                2;
                3
            )),
        ),
        (
            // SERIES with size 0 is allowed
            "SELECT * FROM SERIES(0)",
            Ok(Payload::Select {
                labels: vec!["N".into()],
                rows: Vec::new(),
            }),
        ),
        (
            // SERIES without parentheses is a normal table name
            "SELECT * FROM SERIES",
            Err(FetchError::TableNotFound("SERIES".into()).into()),
        ),
        (
            // SERIES without size is not allowed
            "SELECT * FROM SERIES()",
            Err(TranslateError::LackOfArgs.into()),
        ),
        (
            // SERIES with unary minus is not allowed
            "SELECT * FROM SERIES(-1)",
            Err(FetchError::SeriesSizeWrong(-1).into()),
        ),
        (
            // SELECT without Table
            "SELECT 1, 'a', true, 1 + 2, 'a' || 'b'",
            Ok(select!(
                "1"   | "'a'"      | "true" | "1 + 2" | "'a' || 'b'"
                I64   | Str        | Bool   | I64     | Str;
                1       "a".into()   true     3         "ab".into()
            )),
        ),
        (
            // SELECT without Table in Scalar subquery
            r#"SELECT (SELECT "Hello")"#,
            Ok(select!(
                r#"(SELECT "Hello")"#
                Str;
                "Hello".to_owned()
            )),
        ),
        (
            // SELECT without Table with Column aliases
            "SELECT 1 AS id, (SELECT MAX(N) FROM SERIES(3)) AS max",
            Ok(select!(
                id  | max
                I64 | I64;
                1     3
            )),
        ),
        (
            // SELECT without Table in Drived
            "SELECT * FROM (SELECT 1) AS Drived",
            Ok(select!(
                "1"
                I64;
                1
            )),
        ),
        (
            // `SELECT *` fetches column `N` for now
            "SELECT *",
            Ok(select!(
                N
                I64;
                1
            )),
        ),
        (
            // CTAS without Table
            "CREATE TABLE TargetTable AS SELECT 1",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TargetTable",
            Ok(select!(
                N
                I64;
                1
            )),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
