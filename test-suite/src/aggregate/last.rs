use {crate::*, gluesql_core::prelude::Value::*};

test_case!(last, async move {
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
            "SELECT LAST(age) FROM Item",
            select_with_null!(
                "LAST(age)";
                Null
            ),
        ),
        (
            "SELECT LAST(id), LAST(quantity) FROM Item",
            select!(
                "LAST(id)" | "LAST(quantity)"
                I64        | I64;
                5            25
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }
});
