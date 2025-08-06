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
            (5, 25, NULL, 1),
            (6, 15,   11, 2),
            (7, 20,   90, 1);
    ",
    )
    .await;

    let test_cases = [
        (
            "SELECT COUNT(*) FROM Item;",
            select!(
                "COUNT(*)";
                I64;
                7
            ),
        ),
        (
            "SELECT COUNT(age), COUNT(quantity) FROM Item;",
            select!(
                "COUNT(age)" | "COUNT(quantity)";
                I64          |               I64;
                5                              6
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
            select!("COUNT(DISTINCT id)"; I64; 7),
        ),
        (
            "SELECT COUNT(DISTINCT age) FROM Item",
            select!("COUNT(DISTINCT age)"; I64; 3),
        ),
        (
            "SELECT COUNT(age), COUNT(DISTINCT age) FROM Item",
            select!(
                "COUNT(age)" | "COUNT(DISTINCT age)";
                I64          |                  I64;
                5                                  3
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
