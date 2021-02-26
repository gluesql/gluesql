use crate::*;

test_case!(error, async move {
    run!("CREATE TABLE TableA (id INTEGER);");
    run!("INSERT INTO TableA (id) VALUES (1);");

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
            EvaluateError::ValueNotFound("noname".to_owned()).into(),
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
        #[cfg(feature = "alter-table")]
        (
            ExecuteError::UnsupportedAlterTableOperation(
                r#"ADD CONSTRAINT "hey" PRIMARY KEY (asdf)"#.to_owned(),
            )
            .into(),
            r#"ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf);"#,
        ),
    ];

    for (error, sql) in test_cases.into_iter() {
        test!(Err(error), sql);
    }

    run!("CREATE TABLE TableB (id BOOL);");
    test!(
        Err(ValueError::SqlTypeNotSupported.into()),
        "INSERT INTO TableB (id) VALUES (0);"
    );

    run!("CREATE TABLE TableA (id INTEGER UNIQUE);");
    run!("INSERT INTO TableA (id) VALUES (1);");
    test!(
        Err(ValidateError::DuplicateEntryOnUniqueField("1".to_string(), "id".to_string()).into()),
        "INSERT INTO TableA (id) VALUES (1)"
    );
    test!(
        Err(ValidateError::IncompatibleTypeOnTypedField("A".to_string(), "id".to_string()).into()),
        "INSERT INTO TableA (id) VALUES (\"A\")"
    );
});
