mod helper;

use gluesql::{Payload, Row, Value, ValueError};
use helper::{Helper, SledHelper};

#[test]
fn nullable() {
    let helper = SledHelper::new("data/nullable");

    helper.run_and_print(
        r#"
CREATE TABLE Test (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)"#,
    );
    helper.run_and_print("INSERT INTO Test (id, num, name) VALUES (NULL, 2, \"Hello\")");
    helper.run_and_print("INSERT INTO Test (id, num, name) VALUES (1, 9, \"World\")");
    helper.run_and_print("INSERT INTO Test (id, num, name) VALUES (3, 4, \"Great\")");

    use Value::*;

    let found = helper
        .run("SELECT id, num, name FROM Test")
        .expect("select");
    let expected = select!(
        OptI64  I64 Str;
        None    2   "Hello".to_owned();
        Some(1) 9   "World".to_owned();
        Some(3) 4   "Great".to_owned()
    );
    assert_eq!(expected, found);

    let found = helper
        .run("SELECT id, num FROM Test WHERE id = NULL AND name = \'Hello\'")
        .expect("select");
    let expected = select!(OptI64 I64; None 2);
    assert_eq!(expected, found);

    helper.run_and_print("UPDATE Test SET id = 2");

    let found = helper.run("SELECT id FROM Test").expect("select");
    let expected = select!(OptI64; Some(2); Some(2); Some(2));
    assert_eq!(expected, found);

    let found = helper.run("SELECT id, num FROM Test").expect("select");
    let expected = select!(OptI64 I64; Some(2) 2; Some(2) 9; Some(2) 4);
    assert_eq!(expected, found);

    let found = helper.run("INSERT INTO Test VALUES (1, NULL)");
    let expected = Err(ValueError::NullValueOnNotNullField.into());
    assert_eq!(expected, found);
}
