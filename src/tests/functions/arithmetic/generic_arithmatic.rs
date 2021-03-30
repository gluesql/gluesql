use {crate::*, std::borrow::Cow};

test_case!(async move {
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
        count!(*num, sql);
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
        (
            LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", data::Literal::Boolean(true)),
                format!("{:?}", data::Literal::Number(Cow::Owned("1".to_owned()))),
            )
            .into(),
            "SELECT * FROM Arith WHERE TRUE + 1 = 1",
        ),
    ];

    for (error, sql) in test_cases.into_iter() {
        test!(Err(error), sql);
    }
});
