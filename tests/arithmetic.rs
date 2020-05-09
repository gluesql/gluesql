mod helper;

use gluesql::{Value, ValueError};

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
    ];

    for (num, sql) in test_cases.iter() {
        helper.test_rows(sql, *num);
    }

    helper.test_error(
        "SELECT * FROM Arith WHERE name + id < 1",
        ValueError::AddOnNonNumeric(Value::String("A".to_string()), Value::I64(1)).into(),
    );
}
