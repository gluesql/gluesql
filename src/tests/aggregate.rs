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
        ("SELECT count(*) FROM Item", select!(I64; 5)),
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
                OptI64 OptI64   OptI64;
                None   Some(90) Some(3)
            ),
        ),
        (
            "SELECT SUM(age) + SUM(quantity) FROM Item",
            select!(OptI64; None),
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

pub fn group_by(mut tester: impl tests::Tester) {
    let create_sql = "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER NULL,
            city TEXT,
            ratio FLOAT,
        );
    ";

    tester.run_and_print(create_sql);

    let insert_sqls = [
        "INSERT INTO Item (id, quantity, city, ratio) VALUES (1, 10, \"Seoul\", 0.2);",
        "INSERT INTO Item (id, quantity, city, ratio) VALUES (2, 0, \"Dhaka\", 0.9);",
        "INSERT INTO Item (id, quantity, city, ratio) VALUES (3, NULL, \"Beijing\", 1.1);",
        "INSERT INTO Item (id, quantity, city, ratio) VALUES (3, 30, \"Daejeon\", 3.2);",
        "INSERT INTO Item (id, quantity, city, ratio) VALUES (4, 11, \"Seoul\", 11.1);",
        "INSERT INTO Item (id, quantity, city, ratio) VALUES (5, 24, \"Seattle\", 6.11);",
    ];

    for insert_sql in insert_sqls.iter() {
        tester.run(insert_sql).unwrap();
    }

    let mut run = |sql| tester.run(sql).expect("select");

    use Value::*;

    let test_cases = vec![
        (
            "SELECT id, COUNT(*) FROM Item GROUP BY id",
            select!(I64 I64; 1 1; 2 1; 3 2; 4 1; 5 1),
        ),
        (
            "SELECT id FROM Item GROUP BY id",
            select!(I64; 1; 2; 3; 4; 5),
        ),
        (
            "SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city",
            select!(
                 OptI64   I64 Str;
                 Some(21) 2 "Seoul".to_owned();
                 Some(0)  1 "Dhaka".to_owned();
                 None     1 "Beijing".to_owned();
                 Some(30) 1 "Daejeon".to_owned();
                 Some(24) 1 "Seattle".to_owned()
            ),
        ),
        (
            "SELECT id, city FROM Item GROUP BY city",
            select!(
                 I64 Str;
                 1 "Seoul".to_owned();
                 2 "Dhaka".to_owned();
                 3 "Beijing".to_owned();
                 3 "Daejeon".to_owned();
                 5 "Seattle".to_owned()
            ),
        ),
        (
            "SELECT ratio FROM Item GROUP BY id, city",
            select!(F64; 0.2; 0.9; 1.1; 3.2; 11.1; 6.11),
        ),
        (
            "SELECT ratio FROM Item GROUP BY id, city HAVING ratio > 10",
            select!(F64; 11.1),
        ),
        (
            "SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city HAVING COUNT(*) > 1",
            select!(
                 OptI64   I64 Str;
                 Some(21) 2 "Seoul".to_owned()
            ),
        ),
    ];

    test_cases
        .into_iter()
        .for_each(|(sql, expected)| assert_eq!(expected, run(sql)));

    let error_cases = vec![(
        ValueError::FloatCannotBeGroupedBy.into(),
        "SELECT * FROM Item GROUP BY ratio;",
    )];

    error_cases
        .into_iter()
        .for_each(|(error, sql)| tester.test_error(sql, error));
}
