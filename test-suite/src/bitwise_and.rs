use gluesql_core::error::LiteralError;

use {crate::*, gluesql_core::prelude::Value::*};

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
            LiteralError::UnsupportedBinaryArithmetic(
                "Number(BigDecimal(\"1.1\"))".to_owned(),
                "Number(BigDecimal(\"12\"))".to_owned()
            ).into())
    );
});
