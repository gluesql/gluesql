mod helper;

use gluesql::{Payload, Row, Value};
use helper::{Helper, SledHelper};

#[test]
fn insert_select() {
    println!("\n\n");

    let helper = SledHelper::new("data.db");

    helper.run_and_print(
        r#"
CREATE TABLE test (
    id INTEGER,
)"#,
    );
    helper.run_and_print("insert into test (id) values (1)");

    let res = helper.run("select id from test").expect("select");
    assert_eq!(res, Payload::Select(vec![Row(vec![Value::I64(1)])]));

    helper.run_and_print("update test set id = 2");

    let res = helper.run("select id from test").expect("select");
    assert_eq!(res, Payload::Select(vec![Row(vec![Value::I64(2)])]));
}
