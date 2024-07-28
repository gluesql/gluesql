use {crate::*, gluesql_core::prelude::Value::*};

test_case!(variance, {
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
            "SELECT VARIANCE(age) FROM Item",
            select_with_null!("VARIANCE(age)"; Null),
        ),
        (
            "SELECT VARIANCE(id), VARIANCE(quantity) FROM Item",
            select!(
                "VARIANCE(id)" | "VARIANCE(quantity)"
                F64            | F64;
                2.0              74.64
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
