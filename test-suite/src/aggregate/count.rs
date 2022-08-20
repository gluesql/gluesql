use {crate::*, gluesql_core::prelude::Value::*};

test_case!(count, async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
            total INTEGER,
        );
    "
    );
    run!(
        "
        INSERT INTO Item (id, quantity, age, total) VALUES
            (1, 10,   11, 1),
            (2,  0,   90, 2),
            (3,  9, NULL, 3),
            (4,  3,    3, 1),
            (5, 25, NULL, 1);
    "
    );

    let test_cases = vec![
        ("SELECT COUNT(*) FROM Item", select!("COUNT(*)"; I64; 5)),
        ("SELECT count(*) FROM Item", select!("count(*)"; I64; 5)),
        (
            "SELECT COUNT(*), COUNT(*) FROM Item",
            select!("COUNT(*)" | "COUNT(*)"; I64 | I64; 5 5),
        ),
        (
            "SELECT COUNT(age), COUNT(quantity) FROM Item",
            select!("COUNT(age)" | "COUNT(quantity)"; I64 | I64; 3 5),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }
});
