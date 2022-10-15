use {crate::*, gluesql_core::prelude::Value::*};

test_case!(min, async move {
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
    let test_cases = [
        (
            "SELECT MIN(age) FROM Item",
            select_with_null!(
                "MIN(age)";
                I64(3)
            ),
        ),
        (
            "SELECT MIN(id), MIN(quantity) FROM Item",
            select!(
                "MIN(id)" | "MIN(quantity)"
                I64       | I64;
                1           0
            ),
        ),
        (
            "SELECT MIN(id + quantity) FROM Item;",
            select!(
                "MIN(id + quantity)"
                I64;
                2
            ),
        ),
        (
            "SELECT SUM(quantity) * 2 + MIN(quantity) - 3 / 1 FROM Item;",
            select!(
                "SUM(quantity) * 2 + MIN(quantity) - 3 / 1"
                I64;
                91
            ),
        ),
        (
            "SELECT MIN(CASE WHEN quantity > 5 THEN id END) FROM Item;",
            select!(
                "MIN(CASE WHEN quantity > 5 THEN id END)"
                I64;
                1
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }
});
