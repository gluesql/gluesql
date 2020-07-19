mod helper;

use gluesql::{AggregateError, BlendContextError, Payload, Row, Value};
use helper::{Helper, SledHelper};

#[test]
fn aggregate() {
    let helper = SledHelper::new("data/aggregate");

    let create_sql = "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
        );
    ";

    helper.run_and_print(create_sql);

    let insert_sqls = [
        "INSERT INTO Item (id, quantity) VALUES (1, 10);",
        "INSERT INTO Item (id, quantity) VALUES (2, 0);",
        "INSERT INTO Item (id, quantity) VALUES (3, 9);",
        "INSERT INTO Item (id, quantity) VALUES (4, 3);",
        "INSERT INTO Item (id, quantity) VALUES (5, 25);",
    ];

    for insert_sql in insert_sqls.iter() {
        helper.run(insert_sql).unwrap();
    }

    use Value::*;

    let run = |sql| helper.run(sql).expect("select");

    let test_cases = vec![
        ("SELECT COUNT(*) FROM Item", select!(I64; 5)),
        ("SELECT COUNT(*), COUNT(*) FROM Item", select!(I64 I64; 5 5)),
        (
            "SELECT SUM(quantity), MAX(quantity), MIN(quantity) FROM Item",
            select!(I64 I64 I64; 47 25 0),
        ),
    ];

    test_cases
        .into_iter()
        .for_each(|(sql, expected)| assert_eq!(expected, run(sql)));

    let error_cases = vec![
        (
            AggregateError::UnsupportedCompoundIdentifier("id.name.ok".to_owned()).into(),
            "SELECT SUM(id.name.ok) FROM Item;",
        ),
        (
            AggregateError::UnsupportedAggregation("WHATEVER".to_owned()).into(),
            "SELECT WHATEVER(*) FROM Item;",
        ),
        (
            AggregateError::OnlyIdentifierAllowed.into(),
            "SELECT SUM(1 + 2) FROM Item;",
        ),
        (
            BlendContextError::ValueNotFound.into(),
            "SELECT SUM(num) FROM Item;",
        ),
    ];

    error_cases
        .into_iter()
        .for_each(|(error, sql)| helper.test_error(sql, error));
}
