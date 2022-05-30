use crate::*;

test_case!(error, async move {
    use gluesql_core::{
        data::RowError,
        executor::{EvaluateError, ExecuteError, FetchError, SelectError},
        translate::TranslateError,
    };

    run!("CREATE TABLE TableA (id INTEGER);");
    run!("INSERT INTO TableA (id) VALUES (1);");
    run!("INSERT INTO TableA (id) VALUES (9);");
    run!("CREATE TABLE TableZ (id INTEGER);");
    run!("INSERT INTO TableZ (id) VALUES (9);");

    let test_cases = vec![
        (
            TranslateError::UnsupportedStatement("TRUNCATE TABLE TableA".to_owned()).into(),
            "TRUNCATE TABLE TableA;",
        ),
        (
            TranslateError::UnsupportedBinaryOperator("^".to_owned()).into(),
            "SELECT 1 ^ 2 FROM TableA;",
        ),
        (
            TranslateError::UnsupportedQuerySetExpr(
                "SELECT * FROM TableA UNION SELECT * FROM TableA".to_owned(),
            )
            .into(),
            "SELECT * FROM TableA UNION SELECT * FROM TableA;",
        ),
        #[cfg(feature = "alter-table")]
        (
            TranslateError::UnsupportedAlterTableOperation(
                "ADD CONSTRAINT hello UNIQUE (id)".to_owned(),
            )
            .into(),
            "ALTER TABLE TableA ADD CONSTRAINT hello UNIQUE (id)",
        ),
        (
            TranslateError::UnsupportedExpr("1 COLLATE TableA".to_owned()).into(),
            "SELECT 1 COLLATE TableA FROM TableA;",
        ),
        (
            TranslateError::UnsupportedDateTimeField("MICROSECONDS".to_owned()).into(),
            r#"Select extract(microseconds from "2011-01-1") from TableA;"#,
        ),
        (
            ExecuteError::TableNotFound("Nothing".to_owned()).into(),
            "INSERT INTO Nothing VALUES (1);",
        ),
        (
            ExecuteError::TableNotFound("Nothing".to_owned()).into(),
            "UPDATE Nothing SET a = 1;",
        ),
        (
            FetchError::TableNotFound("Nothing".to_owned()).into(),
            "SELECT * FROM Nothing;",
        ),
        (
            TranslateError::TooManyTables.into(),
            "SELECT * FROM TableA, TableB",
        ),
        (TranslateError::LackOfTable.into(), "SELECT 1;"),
        (
            TranslateError::UnsupportedQueryTableFactor(
                "(SELECT * FROM TableB) AS TableC".to_owned(),
            )
            .into(),
            "SELECT * FROM TableA JOIN (SELECT * FROM TableB) as TableC ON 1 = 1",
        ),
        (
            TranslateError::UnsupportedJoinConstraint("USING".to_owned()).into(),
            "SELECT * FROM TableA JOIN TableA USING (id);",
        ),
        (
            TranslateError::UnsupportedJoinOperator("CrossJoin".to_owned()).into(),
            "SELECT * FROM TableA CROSS JOIN TableA as A;",
        ),
        (
            TranslateError::JoinOnUpdateNotSupported.into(),
            "UPDATE TableA INNER JOIN TableA ON 1 = 1 SET 1 = 1",
        ),
        (
            TranslateError::UnsupportedTableFactor("(SELECT * FROM TableA)".to_owned()).into(),
            "UPDATE (SELECT * FROM TableA) SET 1 = 1",
        ),
        (
            TranslateError::CompoundIdentOnUpdateNotSupported("TableA.id = 1".to_owned()).into(),
            "UPDATE TableA SET TableA.id = 1 WHERE id = 1",
        ),
        (
            EvaluateError::NestedSelectRowNotFound.into(),
            "SELECT * FROM TableA WHERE id = (SELECT id FROM TableA WHERE id = 2);",
        ),
        (
            EvaluateError::MoreThanOneRowReturned.into(),
            "select (select id from TableA) as id from TableA",
        ),
        (
            EvaluateError::ValueNotFound("noname".to_owned()).into(),
            "SELECT * FROM TableA WHERE noname = 1;",
        ),
        (
            RowError::LackOfRequiredColumn("id".to_owned()).into(),
            "INSERT INTO TableA (id2) VALUES (1);",
        ),
        (
            RowError::ColumnAndValuesNotMatched.into(),
            "INSERT INTO TableA (id) VALUES ('test2', 3)",
        ),
        (
            RowError::TooManyValues.into(),
            "INSERT INTO TableA VALUES (100), (100, 200);",
        ),
        (
            TranslateError::InvalidParamsInDropIndex.into(),
            "DROP INDEX TableA",
        ),
        (
            TranslateError::InvalidParamsInDropIndex.into(),
            "DROP INDEX TableA.IndexB.IndexC",
        ),
        (
            SelectError::ColumnReferenceAmbiguous("id".to_string()).into(),
            "SELECT id FROM TableA JOIN TableZ ON TableA.id = TableZ.id",
        ),
        #[cfg(feature = "alter-table")]
        (
            TranslateError::UnsupportedAlterTableOperation(
                r#"ADD CONSTRAINT "hey" PRIMARY KEY (asdf)"#.to_owned(),
            )
            .into(),
            r#"ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf);"#,
        ),
    ];

    for (error, sql) in test_cases {
        test!(Err(error), sql);
    }
});
