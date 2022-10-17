use {crate::*, gluesql_core::prelude::Value::*};

test_case!(sum, async move {
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
            "SELECT SUM(age) FROM Item",
            select_with_null!(
                "SUM(age)";
                Null
            ),
        ),
        (
            "SELECT SUM(id), SUM(quantity) FROM Item",
            select!(
                "SUM(id)" | "SUM(quantity)"
                I64             | I64;
                15                47
            ),
        ),
        (
            "SELECT SUM(ifnull(age, 0)) from Item;",
            select!(
                "SUM(ifnull(age, 0))"
                I64;
                104
            ),
        ),
        (
            "SELECT SUM(1 + 2) FROM Item;",
            select!(
                "SUM(1 + 2)"
                I64;
                15
            ),
        ),
        (
            "SELECT SUM(id + 1) FROM Item;",
            select!(
                "SUM(id + 1)"
                I64;
                20
            ),
        ),
        (
            "SELECT SUM(id * quantity) FROM Item;",
            select!(
                "SUM(id * quantity)"
                I64;
                174
            ),
        ),
        (
            "SELECT SUM(CASE WHEN id > 3 THEN quantity ELSE 0 END) FROM Item;",
            select!(
                "SUM(CASE WHEN id > 3 THEN quantity ELSE 0 END)"
                I64;
                28
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }
});
