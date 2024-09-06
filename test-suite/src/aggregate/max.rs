use {crate::*, gluesql_core::prelude::Value::*};

test_case!(max, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER,
            age INTEGER NULL,
            total INTEGER
        );
    ",
    )
    .await;
    g.run(
        "
        INSERT INTO Item (id, quantity, age, total) VALUES
            (1, 10,   11, 1),
            (2,  0,   90, 2),
            (3,  9, NULL, 3),
            (4,  3,    3, 1),
            (5, 25, NULL, 1);
    ",
    )
    .await;

    let test_cases = [
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
            "SELECT MAX(id - quantity) FROM Item;",
            select!(
                "MAX(id - quantity)"
                I64;
                2
            ),
        ),
        (
            "SELECT SUM(quantity) * 2 + MAX(quantity) - 3 / 1 FROM Item",
            select!("SUM(quantity) * 2 + MAX(quantity) - 3 / 1"; I64; 116),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
