use {crate::*, gluesql_core::prelude::Value::*};

test_case!(count, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER NULL,
            age INTEGER NULL,
            total INTEGER
        );
    ",
    )
    .await;
    g.run(
        "
        INSERT INTO Item (id, quantity, age, total) VALUES
            (1, NULL,   11, 1),
            (2,  0,   90, 2),
            (3,  9, NULL, 3),
            (4,  3,    3, 1),
            (5, 25, NULL, 1);
    ",
    )
    .await;

    let test_cases = [
        (
            "SELECT COUNT(*) FROM Item;",
            select!(
                "COUNT(*)";
                I64;
                5
            ),
        ),
        (
            "SELECT COUNT(age), COUNT(quantity) FROM Item;",
            select!(
                "COUNT(age)" | "COUNT(quantity)";
                I64          |               I64;
                3                              4
            ),
        ),
        (
            "SELECT COUNT(NULL);",
            select!(
                "COUNT(NULL)";
                I64;
                0
            ),
        ),
        (
            "SELECT COUNT(DISTINCT id) FROM Item",
            select!("COUNT(DISTINCT id)"; I64; 5),
        ),
        (
            "SELECT COUNT(DISTINCT age) FROM Item",
            select!("COUNT(DISTINCT age)"; I64; 3), // NULL 무시하고 [11, 90, 3] = 3개
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
