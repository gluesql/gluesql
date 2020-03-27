mod helper;

use gluesql::{BlendError, ExecuteError, SelectError};
use helper::{Helper, SledHelper};

#[test]
fn error() {
    let helper = SledHelper::new("data.db");

    let sql = "DROP TABLE TableA";
    helper.test_error(sql, ExecuteError::QueryNotSupported.into());

    helper.run_and_print("CREATE TABLE TableA (id INTEGER);");
    helper.run_and_print("INSERT INTO TableA (id) VALUES (1);");

    let test_cases = {
        use SelectError::*;

        vec![
            (TableNotFound, "SELECT * FROM;"),
            (TooManyTables, "SELECT * FROM TableA, TableB"),
            (
                JoinRightSideNotSupported,
                "SELECT * FROM TableA JOIN (SELECT * FROM TableB) as TableC ON 1 = 1",
            ),
            (
                UsingOnJoinNotSupported,
                "SELECT * FROM TableA JOIN TableA USING (id)",
            ),
            (
                JoinTypeNotSupported,
                "SELECT * FROM TableA CROSS JOIN TableA as A ON 1 = 2;",
            ),
        ]
    };

    for (error, sql) in test_cases.into_iter() {
        helper.test_error(sql, error.into());
    }

    helper.test_error(
        "SELECT TableA.* FROM TableA;",
        BlendError::FieldDefinitionNotSupported.into(),
    );
}
