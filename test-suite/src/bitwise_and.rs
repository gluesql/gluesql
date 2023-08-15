use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{ast::BinaryOperator, data::Literal, error::LiteralError, prelude::Value::*},
    std::{borrow::Cow, str::FromStr},
};

test_case!(bitwise_and, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Test (
            id INTEGER,
            lhs INTEGER,
            rhs INTEGER
        );
        ",
    )
    .await;

    g.run(
        "
        INSERT INTO Test
        VALUES
            (1, 29, 15);
        ",
    )
    .await;

    g.named_test(
        "bitwise-and for values",
        "SELECT lhs & rhs AS and_result FROM Test",
        Ok(select!(and_result I64; 13)),
    )
    .await;

    g.named_test(
        "bitwise-and for literals",
        "SELECT 29 & 15 AS column1;",
        Ok(select!(column1 I64; 13)),
    )
    .await;

    g.named_test(
        "bitwise-and between a value and a literal",
        "SELECT 29 & rhs AS and_result FROM Test",
        Ok(select!(and_result I64; 13)),
    )
    .await;

    g.named_test(
        "bitwise_and between multiple values",
        "SELECT 29 & rhs & 3 AS and_result FROM Test",
        Ok(select!(and_result I64; 1)),
    )
    .await;

    g.named_test(
        "bitwise_and between wrong type values shoud occurs error",
        "SELECT 1.1 & 12 AS and_result FROM Test",
        Err(LiteralError::UnsupportedBinaryOperation {
            left: format!(
                "{:?}",
                Literal::Number(Cow::Owned(BigDecimal::from_str("1.1").unwrap()))
            ),
            op: BinaryOperator::BitwiseAnd,
            right: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(12)))),
        }
        .into()),
    )
    .await;

    // About NULL
    g.named_test(
        "bitwise_and between null and value",
        "SELECT null & rhs AS and_result from Test",
        Ok(select_with_null!(and_result; Null)),
    )
    .await;
    g.named_test(
        "bitwise_and between value and null",
        "SELECT rhs & null AS and_result from Test",
        Ok(select_with_null!(and_result; Null)),
    )
    .await;
    g.named_test(
        "bitwise_and between null and literal",
        "SELECT null & 12 AS and_result from Test",
        Ok(select_with_null!(and_result; Null)),
    )
    .await;
    g.named_test(
        "bitwise_and between literal and null",
        "SELECT 12 & null AS and_result from Test",
        Ok(select_with_null!(and_result; Null)),
    )
    .await;

    g.named_test(
        "bitwise_and for unsupported value",
        "SELECT 'ss' & 'sp' AS and_result from Test",
        Err(LiteralError::UnsupportedBinaryOperation {
            left: format!("{:?}", Literal::Text(Cow::Owned("ss".to_owned()))),
            op: BinaryOperator::BitwiseAnd,
            right: format!("{:?}", Literal::Text(Cow::Owned("sp".to_owned()))),
        }
        .into()),
    )
    .await;
});
