use {crate::*, gluesql_core::prelude::Value::*};

test_case!(total, {
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
            "SELECT TOTAL(age) FROM Item",
            select!(
                "TOTAL(age)"
                F64;
                104.0
            ),
        ),
        (
            "SELECT TOTAL(id), TOTAL(quantity) FROM Item",
            select!(
                "TOTAL(id)" | "TOTAL(quantity)"
                F64         | F64;
                15.0          47.0
            ),
        ),
        (
            "SELECT TOTAL(ifnull(age, 0)) from Item;",
            select!(
                "TOTAL(ifnull(age, 0))"
                F64;
                104.0
            ),
        ),
        (
            "SELECT TOTAL(1 + 2) FROM Item;",
            select!(
                "TOTAL(1 + 2)"
                F64;
                15.0
            ),
        ),
        (
            "SELECT TOTAL(id * quantity) FROM Item;",
            select!(
                "TOTAL(id * quantity)"
                F64;
                174.0
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
