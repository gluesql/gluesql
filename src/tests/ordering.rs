use crate::*;

pub fn ordering(mut tester: impl tests::Tester) {
    let create_sql = "
        CREATE TABLE Operator (
            id INTEGER,
            name TEXT,
        );
    ";

    tester.run_and_print(create_sql);

    let delete_sql = "DELETE FROM Operator";
    tester.run_and_print(delete_sql);

    let insert_sqls = [
        "INSERT INTO Operator (id, name) VALUES (1, \"Abstract\");",
        "INSERT INTO Operator (id, name) VALUES (2, \"Azzzz\");",
        "INSERT INTO Operator (id, name) VALUES (3, \"July\");",
        "INSERT INTO Operator (id, name) VALUES (4, \"Romeo\");",
        "INSERT INTO Operator (id, name) VALUES (5, \"Trade\");",
    ];

    for insert_sql in insert_sqls.iter() {
        tester.run(insert_sql).unwrap();
    }

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
            "SELECT * FROM Operator o1 WHERE 3 > (SELECT id FROM Operator WHERE o1.id < 100);",
        ),
        (2, "SELECT * FROM Operator WHERE name < \"Azzzzzzzzzz\";"),
        (1, "SELECT * FROM Operator WHERE name < \"Az\";"),
        (5, "SELECT * FROM Operator WHERE name < \"zz\";"),
        (5, "SELECT * FROM Operator WHERE \"aa\" < \"zz\";"),
        (4, "SELECT * FROM Operator WHERE \"Romeo\" >= name;"),
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
            "SELECT * FROM Operator WHERE \"zz\" > (SELECT name FROM Operator LIMIT 1)",
        ),
        (
            5,
            "SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) < \"zz\"",
        ),
        (5, "SELECT * FROM Operator WHERE NOT (1 != 1);"),
    ];

    for (num, sql) in test_cases.iter() {
        tester.test_rows(sql, *num);
    }
}
