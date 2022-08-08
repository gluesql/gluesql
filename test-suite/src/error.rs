use crate::*;

test_case!(error, async move {
    use gluesql_core::{
        data::RowError,
        executor::{EvaluateError, ExecuteError, FetchError},
        plan::PlanError,
        translate::TranslateError,
    };

    run!("CREATE TABLE TableA (id INTEGER);");
    run!("INSERT INTO TableA (id) VALUES (1);");
    run!("INSERT INTO TableA (id) VALUES (9);");

    // To test `PlanError` while using `JOIN`
    run!("CREATE TABLE users (id INTEGER, name TEXT);");
    run!(r#"INSERT INTO users (id, name) VALUES (1, "Harry");"#);
    run!("CREATE TABLE testers (id INTEGER, nickname TEXT);");
    run!(r#"INSERT INTO testers (id, nickname) VALUES (1, "Ron");"#);

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
            FetchError::TableNotFound("Nothing".to_owned()).into(),
            "SELECT * FROM Nothing;",
        ),
        (
            TranslateError::TooManyTables.into(),
            "SELECT * FROM TableA, TableB",
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
        #[cfg(feature = "alter-table")]
        (
            TranslateError::UnsupportedAlterTableOperation(
                r#"ADD CONSTRAINT "hey" PRIMARY KEY (asdf)"#.to_owned(),
            )
            .into(),
            r#"ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf);"#,
        ),
        (
            PlanError::ColumnReferenceAmbiguous("id".to_owned()).into(),
            "SELECT id FROM users JOIN testers ON users.id = testers.id;",
        ),
    ];

    for (error, sql) in test_cases {
        test!(Err(error), sql);
    }
});
