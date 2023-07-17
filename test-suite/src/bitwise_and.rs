use {
    bigdecimal::{BigDecimal, FromPrimitive},
    crate::*,
    gluesql_core::{
        ast::{AstLiteral, Expr, BinaryOperator},
        error::LiteralError,
        prelude::Value::*,
    },
};

test_case!(bitwise_and, async move {
    run!(
        "
        CREATE TABLE Test (
            id INTEGER,
            lhs INTEGER,
            rhs INTEGER
        );
        "
    );

    run!(
        "
        INSERT INTO Test
        VALUES
            (1, 29, 15);
        "
    );

    test! (
        name: "bitwise-and for values",
        sql: "SELECT lhs & rhs AS and_result FROM Test",
        expected: Ok(select!(and_result I64; 13))
    );

    test! (
        name: "bitwise-and for literals",
        sql : "SELECT 29 & 15 AS column1;",
        expected : Ok(select!(column1 I64; 13))
    );

    test! (
        name: "bitwise-and between a value and a literal",
        sql: "SELECT 29 & rhs AS and_result FROM Test",
        expected: Ok(select!(and_result I64; 13))
    );

    test! (
        name: "bitwise_and between multiple values",
        sql: "SELECT 29 & rhs & 3 AS and_result FROM Test",
        expected: Ok(select!(and_result I64; 1))
    );

    test! (
        name: "bitwise_and between wrong type values shoud occurs error",
        sql: "SELECT 1.1 & 12 AS and_result FROM Test",
        expected: Err(
            LiteralError::UnsupportedBinaryOperation( Expr::BinaryOp {
                left: Box::new(Expr::Literal(AstLiteral::Number(BigDecimal::from_f64(1.1).unwrap()))),
                op: BinaryOperator::BitwiseAnd,
                right: Box::new(Expr::Literal(AstLiteral::Number(BigDecimal::from_i32(12).unwrap())))
            }).into()
        )
    );

    // About NULL
    test! (
        name: "bitwise_and between null and value",
        sql: "SELECT null & rhs AS and_result from Test",
        expected: Ok(select_with_null!(and_result; Null))
    );
    test! (
        name: "bitwise_and between value and null",
        sql: "SELECT rhs & null AS and_result from Test",
        expected: Ok(select_with_null!(and_result; Null))
    );
    test! (
        name: "bitwise_and between null and literal",
        sql: "SELECT null & 12 AS and_result from Test",
        expected: Ok(select_with_null!(and_result; Null))
    );
    test! (
        name: "bitwise_and between literal and null",
        sql: "SELECT 12 & null AS and_result from Test",
        expected: Ok(select_with_null!(and_result; Null))
    );

    test! (
        name: "bitwise_and for unsupported value",
        sql: "SELECT 'ss' & 'sp' AS and_result from Test",
        expected: Err(
            LiteralError::UnsupportedBinaryOperation( Expr::BinaryOp {
                left: Box::new(Expr::Literal(AstLiteral::QuotedString("ss".to_owned()))),
                op: BinaryOperator::BitwiseAnd,
                right: Box::new(Expr::Literal(AstLiteral::QuotedString("sp".to_owned()))),
            }).into()
        )
    );
});
