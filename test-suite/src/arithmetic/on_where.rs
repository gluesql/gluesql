use crate::*;

test_case!(on_where, async move {
    run!(
        "
        CREATE TABLE Arith (
            id INTEGER,
            num INTEGER,
            name TEXT,
        );
    "
    );
    run!("DELETE FROM Arith");
    run!(
        "
        INSERT INTO Arith (id, num, name) VALUES
            (1, 6, \"A\"),
            (2, 8, \"B\"),
            (3, 4, \"C\"),
            (4, 2, \"D\"),
            (5, 3, \"E\");
    "
    );

    let test_cases = [
        // add on WHERE
        (1, "SELECT * FROM Arith WHERE id = 1 + 1;"),
        (5, "SELECT * FROM Arith WHERE id < id + 1;"),
        (5, "SELECT * FROM Arith WHERE id < num + id;"),
        (3, "SELECT * FROM Arith WHERE id + 1 < 5;"),
        // subtract on WHERE
        (1, "SELECT * FROM Arith WHERE id = 2 - 1;"),
        (1, "SELECT * FROM Arith WHERE 2 - 1 = id;"),
        (5, "SELECT * FROM Arith WHERE id > id - 1;"),
        (5, "SELECT * FROM Arith WHERE id > id - num;"),
        (3, "SELECT * FROM Arith WHERE 5 - id < 3;"),
        // multiply on WHERE
        (1, "SELECT * FROM Arith WHERE id = 2 * 2;"),
        (0, "SELECT * FROM Arith WHERE id > id * 2;"),
        (0, "SELECT * FROM Arith WHERE id > num * id;"),
        (1, "SELECT * FROM Arith WHERE 3 * id < 4;"),
        // divide on WHERE
        (0, "SELECT * FROM Arith WHERE id = 5 / 2;"),
        (5, "SELECT * FROM Arith WHERE id > id / 2;"),
        (3, "SELECT * FROM Arith WHERE id > num / id;"),
        (2, "SELECT * FROM Arith WHERE 10 / id = 2;"),
        // modulo on WHERE
        (1, "SELECT * FROM Arith WHERE id = 5 % 2;"),
        (5, "SELECT * FROM Arith WHERE id > num % id;"),
        (1, "SELECT * FROM Arith WHERE num % id > 2;"),
        (2, "SELECT * FROM Arith WHERE num % 3 < 2 % id;"),
        // etc
        (1, "SELECT * FROM Arith WHERE 1 + 1 = id;"),
        (5, "UPDATE Arith SET id = id + 1;"),
        (0, "SELECT * FROM Arith WHERE id = 1;"),
        (4, "UPDATE Arith SET id = id - 1 WHERE id != 6;"),
        (2, "SELECT * FROM Arith WHERE id <= 2;"),
        (5, "UPDATE Arith SET id = id * 2;"),
        (5, "UPDATE Arith SET id = id / 2;"),
        (2, "SELECT * FROM Arith WHERE id <= 2;"),
    ];

    for (num, sql) in test_cases.iter() {
        count!(*num, sql);
    }
});
