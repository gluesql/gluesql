mod helper;

use gluesql::ExecuteError;
use helper::{Helper, SledHelper};

#[test]
fn error() {
    let helper = SledHelper::new("data.db");

    let sql = "DROP TABLE tableA";
    helper.test_error(sql, ExecuteError::QueryNotSupported.into());
}
