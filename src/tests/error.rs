use crate::*;

pub fn error(mut tester: impl tests::Tester) {
    tester.run_and_print("CREATE TABLE TableA (id INTEGER);");
    tester.run_and_print("INSERT INTO TableA (id) VALUES (1);");

    let test_cases = vec![
        (ExecuteError::QueryNotSupported.into(), "COMMIT;"),
        (
            FetchError::TableNotFound("Nothing".to_owned()).into(),
            "SELECT * FROM Nothing;",
        ),
        (
            SelectError::TooManyTables.into(),
            "SELECT * FROM TableA, TableB",
        ),
        (
            TableError::TableFactorNotSupported.into(),
            "SELECT * FROM TableA JOIN (SELECT * FROM TableB) as TableC ON 1 = 1",
        ),
        (
            JoinError::UsingOnJoinNotSupported.into(),
            "SELECT * FROM TableA JOIN TableA USING (id);",
        ),
        (
            JoinError::JoinTypeNotSupported.into(),
            "SELECT * FROM TableA CROSS JOIN TableA as A;",
        ),
        (
            EvaluateError::NestedSelectRowNotFound.into(),
            "SELECT * FROM TableA WHERE id = (SELECT id FROM TableA WHERE id = 2);",
        ),
        (
            FilterContextError::ValueNotFound.into(),
            "SELECT * FROM TableA WHERE noname = 1;",
        ),
        (
            RowError::LackOfRequiredColumn("id".to_owned()).into(),
            "INSERT INTO TableA (id2) VALUES (1);",
        ),
        (
            RowError::LackOfRequiredValue("id".to_owned()).into(),
            "INSERT INTO TableA (id2, id) VALUES (100);",
        ),
        (
            RowError::TooManyValues.into(),
            "INSERT INTO TableA VALUES (100), (100, 200);",
        ),
        (
            ExecuteError::UnsupportedInsertValueType("SELECT id FROM TableA".to_owned()).into(),
            "INSERT INTO TableA SELECT id FROM TableA",
        ),
        #[cfg(feature = "alter-table")]
        (
            ExecuteError::UnsupportedAlterTableOperation(
                r#"ADD CONSTRAINT "hey" PRIMARY KEY (asdf)"#.to_owned(),
            )
            .into(),
            r#"ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf);"#,
        ),
    ];

    test_cases
        .into_iter()
        .for_each(|(error, sql)| tester.test_error(sql, error));

    tester.run_and_print("CREATE TABLE TableB (id BOOL);");
    tester.test_error(
        "INSERT INTO TableB (id) VALUES (0);",
        ValueError::SqlTypeNotSupported.into(),
    );
}
