use {crate::*, gluesql_core::prelude::Value::*};
// TODO First & Last 등등
test_case!(first, async move {
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
            "SELECT FIRST(age) FROM Item",
            select_with_null!(
                "FIRST(age)";
                I64(11)
            ),
        ),
        (
            "SELECT FIRST(id), FIRST(quantity) FROM Item",
            select!(
                "FIRST(id)" | "FIRST(quantity)"
                I64         | I64;
                1              10
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }
});
