mod helper;

use gluesql::{UpdateError, ValueError};

use helper::{Helper, SledHelper};

#[test]
fn arithmetic() {
    let helper = SledHelper::new("data.db");

    let create_sql = "
        CREATE TABLE Arith (
            id INTEGER,
            num INTEGER,
            name TEXT,
        );
    ";

    helper.run_and_print(create_sql);

    let delete_sql = "DELETE FROM Arith";
    helper.run_and_print(delete_sql);

    let insert_sqls = [
        "INSERT INTO Arith (id, num, name) VALUES (1, 6, \"A\");",
        "INSERT INTO Arith (id, num, name) VALUES (2, 8, \"B\");",
        "INSERT INTO Arith (id, num, name) VALUES (3, 4, \"C\");",
        "INSERT INTO Arith (id, num, name) VALUES (4, 2, \"D\");",
        "INSERT INTO Arith (id, num, name) VALUES (5, 3, \"E\");",
    ];

    for insert_sql in insert_sqls.iter() {
        helper.run(insert_sql).unwrap();
    }

    let test_cases = [
        (1, "SELECT * FROM Arith WHERE id = 1 + 1;"),
        (5, "SELECT * FROM Arith WHERE id < id + 1;"),
        (3, "SELECT * FROM Arith WHERE id + 1 < 5;"),
        (2, "SELECT * FROM Arith WHERE id >= num;"),
        (5, "UPDATE Arith SET id = id + 1;"),
        (0, "SELECT * FROM Arith WHERE id = 1;"),
        (4, "UPDATE Arith SET id = id - 1 WHERE id != 6;"),
        (2, "SELECT * FROM Arith WHERE id <= 2;"),
        (5, "UPDATE Arith SET id = id * 2;"),
        (5, "UPDATE Arith SET id = id / 2;"),
        (2, "SELECT * FROM Arith WHERE id <= 2;"),
    ];

    for (num, sql) in test_cases.iter() {
        helper.test_rows(sql, *num);
    }

    let test_cases = vec![
        (
            ValueError::AddOnNonNumeric.into(),
            "SELECT * FROM Arith WHERE name + id < 1",
        ),
        (
            ValueError::SubtractOnNonNumeric.into(),
            "UPDATE Arith SET id = name - 1",
        ),
        (
            ValueError::MultiplyOnNonNumeric.into(),
            "UPDATE Arith SET id = name * 1",
        ),
        (
            ValueError::DivideOnNonNumeric.into(),
            "UPDATE Arith SET id = name / 1",
        ),
        (
            UpdateError::ColumnNotFound(String::from("aaa")).into(),
            "UPDATE Arith SET num = aaa +  1",
        ),
        (
            UpdateError::ColumnNotFound(String::from("aaa")).into(),
            "UPDATE Arith SET aaa = 1",
        ),
    ];

    test_cases
        .into_iter()
        .for_each(|(error, sql)| helper.test_error(sql, error));
}
