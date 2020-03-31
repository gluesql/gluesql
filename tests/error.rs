mod helper;

use gluesql::{BlendError, ExecuteError, JoinError, SelectError, StoreError};
use helper::{Helper, SledHelper};

#[test]
fn error() {
    let helper = SledHelper::new("data.db");

    let sql = "DROP TABLE TableA";
    helper.test_error(sql, ExecuteError::QueryNotSupported.into());

    helper.run_and_print("CREATE TABLE TableA (id INTEGER);");
    helper.run_and_print("INSERT INTO TableA (id) VALUES (1);");

    let test_cases = vec![
        (StoreError::SchemaNotFound.into(), "SELECT * FROM Nothing;"),
        (SelectError::TableNotFound.into(), "SELECT * FROM;"),
        (
            SelectError::TooManyTables.into(),
            "SELECT * FROM TableA, TableB",
        ),
        (
            SelectError::JoinRightSideNotSupported.into(),
            "SELECT * FROM TableA JOIN (SELECT * FROM TableB) as TableC ON 1 = 1",
        ),
        (
            JoinError::UsingOnJoinNotSupported.into(),
            "SELECT * FROM TableA JOIN TableA USING (id)",
        ),
        (
            JoinError::JoinTypeNotSupported.into(),
            "SELECT * FROM TableA CROSS JOIN TableA as A ON 1 = 2;",
        ),
    ];

    test_cases
        .into_iter()
        .for_each(|(error, sql)| helper.test_error(sql, error));

    helper.test_error(
        "SELECT TableA.* FROM TableA;",
        BlendError::FieldDefinitionNotSupported.into(),
    );
}
