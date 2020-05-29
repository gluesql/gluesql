mod helper;

use gluesql::{Payload, Row, Value};
use helper::{Helper, SledHelper};

#[test]
fn insert_select() {
    let helper = SledHelper::new("data.db");

    helper.run_and_print(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)"#,
    );
    helper.run_and_print("INSERT INTO Test (id, num, name) VALUES (1, 2, \"Hello\")");
    helper.run_and_print("INSERT INTO Test (id, num, name) VALUES (1, 9, \"World\")");
    helper.run_and_print("INSERT INTO Test (id, num, name) VALUES (3, 4, \"Great\")");

    use Value::*;

    let found = helper
        .run("SELECT id, num, name FROM Test")
        .expect("select");
    let expected = select!(
        I64 I64 String;
        1   2   "Hello".to_string();
        1   9   "World".to_string();
        3   4   "Great".to_string()
    );
    assert_eq!(expected, found);

    helper.run_and_print("UPDATE Test SET id = 2");

    let found = helper.run("SELECT id FROM Test").expect("select");
    let expected = select!(I64; 2; 2; 2);
    assert_eq!(expected, found);

    let found = helper.run("SELECT id, num FROM Test").expect("select");
    let expected = select!(I64 I64; 2 2; 2 9; 2 4);
    assert_eq!(expected, found);
}
