use {crate::*, gluesql_core::prelude::Value::*};

test_case!(max, async move {
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
        (
            "SELECT MAX(age) FROM Item",
            select_with_null!(
                "MAX(age)";
                I64(90)
            ),
        ),
        (
            "SELECT MAX(id), MAX(quantity) FROM Item",
            select!(
                "MAX(id)" | "MAX(quantity)"
                I64       | I64;
                5           25
            ),
        ),
        (
            "SELECT SUM(quantity) * 2 + MAX(quantity) - 3 / 1 FROM Item",
            select!("SUM(quantity) * 2 + MAX(quantity) - 3 / 1"; I64; 116),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }
});
