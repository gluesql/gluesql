use crate::*;

test_case!(async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
        );
    "
    );
    run!(
        "
        INSERT INTO Item (id, quantity, age) VALUES
            (1, 10,   11),
            (2,  0,   90),
            (3,  9, NULL),
            (4,  3,    3),
            (5, 25, NULL);
    "
    );

    use Value::*;

    let test_cases = vec![
        ("SELECT COUNT(*) FROM Item", select!("COUNT(*)"; I64; 5)),
        ("SELECT count(*) FROM Item", select!("count(*)"; I64; 5)),
        (
            "SELECT COUNT(*), COUNT(*) FROM Item",
            select!("COUNT(*)" | "COUNT(*)"; I64 | I64; 5 5),
        ),
        (
            "SELECT SUM(quantity), MAX(quantity), MIN(quantity) FROM Item",
            select!(
                "SUM(quantity)" | "MAX(quantity)" | "MIN(quantity)"
                I64             | I64             | I64;
                47                25                0
            ),
        ),
        (
            "SELECT SUM(quantity) * 2 + MAX(quantity) - 3 / 1 FROM Item",
            select!("SUM(quantity) * 2 + MAX(quantity) - 3 / 1"; I64; 116),
        ),
        (
            "SELECT SUM(age), MAX(age), MIN(age) FROM Item",
            select_with_null!(
                "SUM(age)" | "MAX(age)" | "MIN(age)";
                Null         I64(90)     I64(3)
            ),
        ),
        (
            "SELECT SUM(age) + SUM(quantity) FROM Item",
            select_with_null!("SUM(age) + SUM(quantity)"; Null),
        ),
        (
            "SELECT COUNT(age), COUNT(quantity) FROM Item",
            select!("COUNT(age)" | "COUNT(quantity)"; I64 | I64; 3 5),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(Ok(expected), sql);
    }

    let error_cases = vec![
        (
            AggregateError::UnsupportedCompoundIdentifier("id.name.ok".to_owned()).into(),
            "SELECT SUM(id.name.ok) FROM Item;",
        ),
        (
            AggregateError::UnsupportedAggregation("AVG".to_owned()).into(),
            "SELECT AVG(*) FROM Item;",
        ),
        (
            AggregateError::OnlyIdentifierAllowed.into(),
            "SELECT SUM(1 + 2) FROM Item;",
        ),
        (
            AggregateError::ValueNotFound("num".to_owned()).into(),
            "SELECT SUM(num) FROM Item;",
        ),
    ];

    for (error, sql) in error_cases.into_iter() {
        test!(Err(error), sql);
    }
});
