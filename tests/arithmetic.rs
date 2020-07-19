mod helper;

use gluesql::{Payload, Row, UpdateError, Value, ValueError};
use helper::{Helper, SledHelper};

#[test]
fn arithmetic() {
    let helper = SledHelper::new("data/arithmetic");

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
        (1, "SELECT * FROM Arith WHERE id = 5 / 2;"),
        (5, "SELECT * FROM Arith WHERE id > id / 2;"),
        (3, "SELECT * FROM Arith WHERE id > num / id;"),
        (2, "SELECT * FROM Arith WHERE 10 / id = 2;"),
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
        helper.test_rows(sql, *num);
    }

    let test_cases = vec![
        (
            ValueError::AddOnNonNumeric.into(),
            "SELECT * FROM Arith WHERE name + id < 1",
        ),
        (
            ValueError::SubtractOnNonNumeric.into(),
            "SELECT * FROM Arith WHERE name - id < 1",
        ),
        (
            ValueError::MultiplyOnNonNumeric.into(),
            "SELECT * FROM Arith WHERE name * id < 1",
        ),
        (
            ValueError::DivideOnNonNumeric.into(),
            "SELECT * FROM Arith WHERE name / id < 1",
        ),
        (
            UpdateError::ColumnNotFound("aaa".to_owned()).into(),
            "UPDATE Arith SET aaa = 1",
        ),
    ];

    test_cases
        .into_iter()
        .for_each(|(error, sql)| helper.test_error(sql, error));
}

#[test]
fn blend_arithmetic() {
    let helper = SledHelper::new("data/blend_arithmetic");

    let create_sql = "
        CREATE TABLE Arith (
            id INTEGER,
            num INTEGER,
        );
    ";

    helper.run_and_print(create_sql);

    let delete_sql = "DELETE FROM Arith";
    helper.run_and_print(delete_sql);

    let insert_sqls = [
        "INSERT INTO Arith (id, num) VALUES (1, 6);",
        "INSERT INTO Arith (id, num) VALUES (2, 8);",
        "INSERT INTO Arith (id, num) VALUES (3, 4);",
        "INSERT INTO Arith (id, num) VALUES (4, 2);",
        "INSERT INTO Arith (id, num) VALUES (5, 3);",
    ];

    for insert_sql in insert_sqls.iter() {
        helper.run(insert_sql).unwrap();
    }

    use Value::I64;

    let sql = "SELECT 1 * 2 + 1 - 3 / 1 FROM Arith LIMIT 1;";
    let found = helper.run(sql).expect("select");
    let expected = select!(I64; 0);
    assert_eq!(expected, found);

    let found = helper
        .run("SELECT id, id + 1, id + num, 1 + 1 FROM Arith")
        .expect("select");
    let expected = select!(
        I64 I64 I64 I64;
        1   2   7   2;
        2   3   10  2;
        3   4   7   2;
        4   5   6   2;
        5   6   8   2
    );
    assert_eq!(expected, found);

    let sql = "
      SELECT a.id + b.id
      FROM Arith a
      JOIN Arith b ON a.id = b.id + 1
    ";
    let found = helper.run(sql).expect("select");
    let expected = select!(I64; 3; 5; 7; 9);
    assert_eq!(expected, found);
}
