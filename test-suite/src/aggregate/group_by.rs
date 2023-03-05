use {crate::*, gluesql_core::prelude::Value::*};

test_case!(group_by, async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            quantity INTEGER NULL,
            city TEXT,
            ratio FLOAT,
        );
    "
    );
    run!(
        "
        INSERT INTO Item (id, quantity, city, ratio) VALUES
            (1,   10,   'Seoul',  0.2),
            (2,    0,   'Dhaka', 6.11),
            (3, NULL, 'Beijing',  1.1),
            (3,   30, 'Daejeon',  0.2),
            (4,   11,   'Seoul',  1.1),
            (5,   24, 'Seattle', 6.11);
    "
    );
    let test_cases = [
        (
            "SELECT id, COUNT(*) FROM Item GROUP BY id",
            select!(
                id  | "COUNT(*)";
                I64 | I64;
                1       1;
                2       1;
                3       2;
                4       1;
                5       1
            ),
        ),
        (
            "SELECT id FROM Item GROUP BY id",
            select!(id; I64; 1; 2; 3; 4; 5),
        ),
        (
            "SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city",
            select_with_null!(
                "SUM(quantity)" | "COUNT(*)" | city;
                I64(21)           I64(2)       Str("Seoul".to_owned());
                I64(0)            I64(1)       Str("Dhaka".to_owned());
                Null              I64(1)       Str("Beijing".to_owned());
                I64(30)           I64(1)       Str("Daejeon".to_owned());
                I64(24)           I64(1)       Str("Seattle".to_owned())
            ),
        ),
        (
            "SELECT id, city FROM Item GROUP BY city",
            select!(
                id  | city
                I64 | Str;
                1     "Seoul".to_owned();
                2     "Dhaka".to_owned();
                3     "Beijing".to_owned();
                3     "Daejeon".to_owned();
                5     "Seattle".to_owned()
            ),
        ),
        (
            "SELECT ratio, COUNT(*) FROM Item GROUP BY ratio",
            select!(
                ratio | "COUNT(*)"
                F64   | I64;
                0.2     2;
                6.11    2;
                1.1     2
            ),
        ),
        (
            "SELECT ratio FROM Item GROUP BY id, city",
            select!(ratio; F64; 0.2; 6.11; 1.1; 0.2; 1.1; 6.11),
        ),
        (
            "SELECT id, ratio FROM Item GROUP BY id, city HAVING ratio > 6",
            select!(id | ratio; I64 | F64; 2 6.11; 5 6.11),
        ),
        (
            "SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city HAVING COUNT(*) > 1",
            select!(
                "SUM(quantity)" | "COUNT(*)" | city
                I64             | I64        | Str;
                21                2            "Seoul".to_owned()
            ),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, Ok(expected));
    }

    run!("CREATE TABLE Sub (id INTEGER);");
    run!("INSERT INTO Sub VALUES (101), (102), (103), (104), (105);");
    test! {
        name: "HAVING - nested select context handling edge case",
        sql: "
            SELECT id
            FROM Sub
            WHERE (id - 100) IN (
                SELECT id
                FROM Item
                GROUP BY id
                HAVING id <= 3
            )",
        expected: Ok(select!(id I64; 101; 102; 103))
    };
});
