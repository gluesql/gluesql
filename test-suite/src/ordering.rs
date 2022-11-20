use crate::*;

test_case!(ordering, async move {
    run!(
        "
        CREATE TABLE Operator (
            id INTEGER,
            name TEXT,
        );
    "
    );
    run!("DELETE FROM Operator");
    run!(
        "
        INSERT INTO Operator (id, name) VALUES
            (1, 'Abstract'),
            (2,    'Azzzz'),
            (3,     'July'),
            (4,    'Romeo'),
            (5,    'Trade');
    "
    );

    let test_cases = [
        (1, "SELECT * FROM Operator WHERE id < 2;"),
        (2, "SELECT * FROM Operator WHERE id <= 2;"),
        (3, "SELECT * FROM Operator WHERE id > 2;"),
        (4, "SELECT * FROM Operator WHERE id >= 2;"),
        (1, "SELECT * FROM Operator WHERE 2 > id;"),
        (2, "SELECT * FROM Operator WHERE 2 >= id;"),
        (3, "SELECT * FROM Operator WHERE 2 < id;"),
        (4, "SELECT * FROM Operator WHERE 2 <= id;"),
        (5, "SELECT * FROM Operator WHERE 1 < 3;"),
        (5, "SELECT * FROM Operator WHERE 3 >= 3;"),
        (0, "SELECT * FROM Operator WHERE 3 > 3;"),
        (
            5,
            "SELECT * FROM Operator o1 WHERE 3 > (SELECT MIN(id) FROM Operator WHERE o1.id < 100);",
        ),
        (2, "SELECT * FROM Operator WHERE name < 'Azzzzzzzzzz';"),
        (1, "SELECT * FROM Operator WHERE name < 'Az';"),
        (5, "SELECT * FROM Operator WHERE name < 'zz';"),
        (5, "SELECT * FROM Operator WHERE 'aa' < 'zz';"),
        (4, "SELECT * FROM Operator WHERE 'Romeo' >= name;"),
        (
            1,
            "SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) >= name",
        ),
        (
            1,
            "SELECT * FROM Operator WHERE name <= (SELECT name FROM Operator LIMIT 1)",
        ),
        (
            5,
            "SELECT * FROM Operator WHERE 'zz' > (SELECT name FROM Operator LIMIT 1)",
        ),
        (
            5,
            "SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) < 'zz'",
        ),
        (5, "SELECT * FROM Operator WHERE NOT (1 != 1);"),
    ];

    for (num, sql) in test_cases {
        count!(num, sql);
    }
});
