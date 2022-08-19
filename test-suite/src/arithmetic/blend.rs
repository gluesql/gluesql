use {
    crate::*,
    gluesql_core::prelude::Value::{self, *},
};

test_case!(blend, async move {
    run!(
        "
        CREATE TABLE Arith (
            id INTEGER,
            num INTEGER,
        );
    "
    );
    run!("DELETE FROM Arith");
    run!(
        "
        INSERT INTO Arith (id, num) VALUES
            (1, 6),
            (2, 8),
            (3, 4),
            (4, 2),
            (5, 3);
    "
    );

    let test_cases = vec![
        (
            "SELECT 1 * 2 + 1 - 3 / 1 FROM Arith LIMIT 1;",
            select!("1 * 2 + 1 - 3 / 1"; I64; 0),
        ),
        (
            "SELECT id, id + 1, id + num, 1 + 1 FROM Arith",
            select!(
                id  | "id + 1" | "id + num" | "1 + 1"
                I64 | I64      | I64        | I64;
                1     2          7            2;
                2     3          10           2;
                3     4          7            2;
                4     5          6            2;
                5     6          8            2
            ),
        ),
        (
            "SELECT a.id + b.id FROM Arith a JOIN Arith b ON a.id = b.id + 1",
            select!("a.id + b.id"; I64; 3; 5; 7; 9),
        ),
        (
            "SELECT TRUE XOR TRUE, FALSE XOR FALSE, TRUE XOR FALSE, FALSE XOR TRUE FROM Arith LIMIT 1",
            select!(
                "true XOR true" | "false XOR false" | "true XOR false" | "false XOR true"
                Value::Bool     | Value::Bool       | Value::Bool      | Value::Bool;
                false             false               true               true
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(Ok(expected), sql);
    }
});
