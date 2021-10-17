use {crate::*, bigdecimal::BigDecimal, std::borrow::Cow};

test_case!(arithmetic, async move {
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

    let test_cases = vec![
        (
            ValueError::AddOnNonNumeric(Value::Str("A".to_owned()), Value::I64(1)).into(),
            "SELECT * FROM Arith WHERE name + id < 1",
        ),
        (
            ValueError::SubtractOnNonNumeric(Value::Str("A".to_owned()), Value::I64(1)).into(),
            "SELECT * FROM Arith WHERE name - id < 1",
        ),
        (
            ValueError::MultiplyOnNonNumeric(Value::Str("A".to_owned()), Value::I64(1)).into(),
            "SELECT * FROM Arith WHERE name * id < 1",
        ),
        (
            ValueError::DivideOnNonNumeric(Value::Str("A".to_owned()), Value::I64(1)).into(),
            "SELECT * FROM Arith WHERE name / id < 1",
        ),
        (
            ValueError::ModuloOnNonNumeric(Value::Str("A".to_owned()), Value::I64(1)).into(),
            "SELECT * FROM Arith WHERE name % id < 1",
        ),
        (
            UpdateError::ColumnNotFound("aaa".to_owned()).into(),
            "UPDATE Arith SET aaa = 1",
        ),
        (
            LiteralError::UnsupportedBinaryArithmetic(
                format!("{:?}", Literal::Boolean(true)),
                format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(1)))),
            )
            .into(),
            "SELECT * FROM Arith WHERE TRUE + 1 = 1",
        ),
        (
            LiteralError::DivisorShouldNotBeZero.into(),
            "SELECT * FROM Arith WHERE id = 2 / 0",
        ),
        (
            LiteralError::DivisorShouldNotBeZero.into(),
            "SELECT * FROM Arith WHERE id = 2 / 0.0",
        ),
        (
            LiteralError::DivisorShouldNotBeZero.into(),
            "SELECT * FROM Arith WHERE id = INTERVAL '2' HOUR / 0",
        ),
        (
            LiteralError::DivisorShouldNotBeZero.into(),
            "SELECT * FROM Arith WHERE id = INTERVAL '2' HOUR / 0.0",
        ),
        (
            LiteralError::DivisorShouldNotBeZero.into(),
            "SELECT * FROM Arith WHERE id = 2 % 0",
        ),
        (
            LiteralError::DivisorShouldNotBeZero.into(),
            "SELECT * FROM Arith WHERE id = 2 % 0.0",
        ),
        (
            EvaluateError::BooleanTypeRequired(format!(
                "{:?}",
                Literal::Text(Cow::Owned("hello".to_owned()))
            ))
            .into(),
            r#"SELECT * FROM Arith WHERE TRUE AND "hello""#,
        ),
        (
            EvaluateError::BooleanTypeRequired(format!("{:?}", Value::Str("A".to_owned()))).into(),
            "SELECT * FROM Arith WHERE name AND id",
        ),
    ];

    for (error, sql) in test_cases {
        test!(Err(error), sql);
    }
});

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

    use Value::I64;

    let sql = "SELECT 1 * 2 + 1 - 3 / 1 FROM Arith LIMIT 1;";
    let found = run!(sql);
    let expected = select!("1 * 2 + 1 - 3 / 1"; I64; 0);
    assert_eq!(expected, found);

    let found = run!("SELECT id, id + 1, id + num, 1 + 1 FROM Arith");
    let expected = select!(
        id  | "id + 1" | "id + num" | "1 + 1"
        I64 | I64      | I64        | I64;
        1     2          7            2;
        2     3          10           2;
        3     4          7            2;
        4     5          6            2;
        5     6          8            2
    );
    assert_eq!(expected, found);

    let sql = "
      SELECT a.id + b.id
      FROM Arith a
      JOIN Arith b ON a.id = b.id + 1
    ";
    let found = run!(sql);
    let expected = select!("a.id + b.id"; I64; 3; 5; 7; 9);
    assert_eq!(expected, found);

    let sql =
        "SELECT TRUE XOR TRUE, FALSE XOR FALSE, TRUE XOR FALSE, FALSE XOR TRUE FROM Arith LIMIT 1";
    let found = run!(sql);
    let expected = select!(
        "true XOR true" | "false XOR false" | "true XOR false" | "false XOR true"
        Value::Bool     | Value::Bool       | Value::Bool      | Value::Bool;
        false             false               true               true
    );
    assert_eq!(expected, found);
});
