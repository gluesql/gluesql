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
        (5, 25, NULL, 1),
        (6, 10,   11, 2),
        (7, 25,   90, 1);
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
                4.0              82.775_510_204_081_63
            ),
        ),
        (
            "SELECT VARIANCE(DISTINCT id) FROM Item",
            select!("VARIANCE(DISTINCT id)"; F64; 4.0),
        ),
        (
            "SELECT VARIANCE(DISTINCT age) FROM Item",
            select_with_null!("VARIANCE(DISTINCT age)"; Null),
        ),
        (
            "SELECT VARIANCE(quantity), VARIANCE(DISTINCT quantity) FROM Item",
            select!(
                "VARIANCE(quantity)"  | "VARIANCE(DISTINCT quantity)";
                F64                   | F64;
                82.775_510_204_081_63   74.64
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, Ok(expected)).await;
    }
});
