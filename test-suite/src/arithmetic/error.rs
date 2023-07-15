use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        ast::{AstLiteral, BinaryOperator, Expr},
        data::{Literal, NumericBinaryOperator},
        error::{EvaluateError, LiteralError, UpdateError, ValueError},
        prelude::Value,
    },
    std::borrow::Cow,
};
test_case!(error, async move {
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
            (1, 6, 'A'),
            (2, 8, 'B'),
            (3, 4, 'C'),
            (4, 2, 'D'),
            (5, 3, 'E');
    "
    );

    let test_cases = [
        (
            "SELECT * FROM Arith WHERE name + id < 1",
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Add,
                rhs: Value::I64(1),
            }
            .into(),
        ),
        (
            "SELECT * FROM Arith WHERE name - id < 1",
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Subtract,
                rhs: Value::I64(1),
            }
            .into(),
        ),
        (
            "SELECT * FROM Arith WHERE name * id < 1",
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Multiply,
                rhs: Value::I64(1),
            }
            .into(),
        ),
        (
            "SELECT * FROM Arith WHERE name / id < 1",
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Divide,
                rhs: Value::I64(1),
            }
            .into(),
        ),
        (
            "SELECT * FROM Arith WHERE name % id < 1",
            ValueError::NonNumericMathOperation {
                lhs: Value::Str("A".to_owned()),
                operator: NumericBinaryOperator::Modulo,
                rhs: Value::I64(1),
            }
            .into(),
        ),
        (
            "UPDATE Arith SET aaa = 1",
            UpdateError::ColumnNotFound("aaa".to_owned()).into(),
        ),
        (
            "SELECT * FROM Arith WHERE TRUE + 1 = 1",
            LiteralError::UnsupportedBinaryOperation(Expr::BinaryOp {
                left: Box::new(Expr::Literal(AstLiteral::Boolean(true))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Literal(AstLiteral::Number(BigDecimal::from(1)))),
            })
            .into(),
        ),
        (
            "SELECT * FROM Arith WHERE id = 2 / 0",
            LiteralError::DivisorShouldNotBeZero.into(),
        ),
        (
            "SELECT * FROM Arith WHERE id = 2 / 0.0",
            LiteralError::DivisorShouldNotBeZero.into(),
        ),
        (
            "SELECT * FROM Arith WHERE id = INTERVAL '2' HOUR / 0",
            ValueError::DivisorShouldNotBeZero.into(),
        ),
        (
            "SELECT * FROM Arith WHERE id = INTERVAL '2' HOUR / 0.0",
            ValueError::DivisorShouldNotBeZero.into(),
        ),
        (
            "SELECT * FROM Arith WHERE id = 2 % 0",
            LiteralError::DivisorShouldNotBeZero.into(),
        ),
        (
            "SELECT * FROM Arith WHERE id = 2 % 0.0",
            LiteralError::DivisorShouldNotBeZero.into(),
        ),
        (
            "SELECT * FROM Arith WHERE TRUE AND 'hello'",
            EvaluateError::BooleanTypeRequired(format!(
                "{:?}",
                Literal::Text(Cow::Owned("hello".to_owned()))
            ))
            .into(),
        ),
        (
            "SELECT * FROM Arith WHERE name AND id",
            EvaluateError::BooleanTypeRequired(format!("{:?}", Value::Str("A".to_owned()))).into(),
        ),
    ];

    for (sql, error) in test_cases {
        test!(sql, Err(error));
    }
});
