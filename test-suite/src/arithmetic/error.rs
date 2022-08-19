use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        data::{Literal, LiteralError, NumericBinaryOperator, ValueError},
        executor::{EvaluateError, UpdateError},
        prelude::Value::{self},
    },
    std::borrow::Cow,
};
test_case!(error_value, async move {
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

    let test_cases = vec![
        (
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Add,
                rhs: Value::I64(1),
            }
            .into(),
            "SELECT * FROM Arith WHERE name + id < 1",
        ),
        (
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Subtract,
                rhs: Value::I64(1),
            }
            .into(),
            "SELECT * FROM Arith WHERE name - id < 1",
        ),
        (
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Multiply,
                rhs: Value::I64(1),
            }
            .into(),
            "SELECT * FROM Arith WHERE name * id < 1",
        ),
        (
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Divide,
                rhs: Value::I64(1),
            }
            .into(),
            "SELECT * FROM Arith WHERE name / id < 1",
        ),
        (
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Modulo,
                rhs: Value::I64(1),
            }
            .into(),
            "SELECT * FROM Arith WHERE name % id < 1",
        ),
    ];

    for (error, sql) in test_cases {
        test!(Err(error), sql);
    }
});

test_case!(error_update, async move {
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

    let test_cases = vec![(
        UpdateError::ColumnNotFound("aaa".to_owned()).into(),
        "UPDATE Arith SET aaa = 1",
    )];

    for (error, sql) in test_cases {
        test!(Err(error), sql);
    }
});

test_case!(error_literal, async move {
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

    let test_cases = vec![
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
    ];

    for (error, sql) in test_cases {
        test!(Err(error), sql);
    }
});

test_case!(error_evaluate, async move {
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

    let test_cases = vec![
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
