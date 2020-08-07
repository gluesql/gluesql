use crate::*;

pub fn aggregate(mut tester: impl tests::Tester) {
    let create_sql = "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
        );
    ";

    tester.run_and_print(create_sql);

    let insert_sqls = [
        "INSERT INTO Item (id, quantity, age) VALUES (1, 10, 11);",
        "INSERT INTO Item (id, quantity, age) VALUES (2, 0, 90);",
        "INSERT INTO Item (id, quantity, age) VALUES (3, 9, NULL);",
        "INSERT INTO Item (id, quantity, age) VALUES (4, 3, 3);",
        "INSERT INTO Item (id, quantity, age) VALUES (5, 25, NULL);",
    ];

    for insert_sql in insert_sqls.iter() {
        tester.run(insert_sql).unwrap();
    }

    use Value::*;

    let mut run = |sql| tester.run(sql).expect("select");

    let test_cases = vec![
        ("SELECT COUNT(*) FROM Item", select!(I64; 5)),
        ("SELECT COUNT(*), COUNT(*) FROM Item", select!(I64 I64; 5 5)),
        (
            "SELECT SUM(quantity), MAX(quantity), MIN(quantity) FROM Item",
            select!(I64 I64 I64; 47 25 0),
        ),
        (
            "SELECT SUM(quantity) * 2 + MAX(quantity) - 3 / 1 FROM Item",
            select!(I64; 116),
        ),
        (
            "SELECT SUM(age), MAX(age), MIN(age) FROM Item",
            select!(
                OptI64    OptI64   OptI64;
                Some(104) Some(90) Some(3)
            ),
        ),
        (
            "SELECT SUM(age) + SUM(quantity) FROM Item",
            select!(OptI64; Some(151)),
        ),
        (
            "SELECT COUNT(age), COUNT(quantity) FROM Item",
            select!(I64 I64; 3 5),
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
        .for_each(|(error, sql)| tester.test_error(sql, error));
}
