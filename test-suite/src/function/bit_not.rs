use {crate::*, gluesql_core::prelude::Value};

test_case!(bit_not, async move {
    test! {
        name: "bit-wise invert all bits: 1 => -2",
        sql: "SELECT BIT_NOT(1) AS test;",
        expected: Ok(select!("test" Value::I64; -2))
    };

    test! {
        name: "bit-wise invert all bits: -2 => 1",
        sql: "SELECT BIT_NOT(CAST(18446744073709551614 AS UINT64)) AS test;",
        expected: Ok(select!("test" Value::U64; 1))
    };

    test! {
        name: "bit-wise invert all bits: 0 => 18446744073709551615",
        sql: "SELECT BIT_NOT(0) AS test;",
        expected: Ok(select!("test" Value::I64; -1))
    };

    test! {
        name: "bit-wise invert all bits: 18446744073709551615 => 0",
        sql: "SELECT BIT_NOT(-1) AS test;",
        expected: Ok(select!("test" Value::I64; 0))
    };

    test! {
        name: "bit-wise invert all bits",
        sql: "SELECT BIT_NOT(CAST(11936128518282651045 AS UINT64)) AS test;",
        expected: Ok(select!("test" Value::U64; 6510615555426900570))
    };

    test! {
        name: "bit-wise invert all bits",
        sql: "SELECT BIT_NOT(CAST(6510615555426900570 AS UINT64)) AS test;",
        expected: Ok(select!("test" Value::U64; 11936128518282651045))
    };

    // operator ~
    test! {
        name: "bit-wise invert all bits: 1 => -2",
        sql: "SELECT ~1 AS test;",
        expected: Ok(select!("test" Value::I64; -2))
    };

    test! {
        name: "bit-wise invert all bits: -2 => 1",
        sql: "SELECT ~-2 AS test;",
        expected: Ok(select!("test" Value::I64; 1))
    };

    test! {
        name: "bit-wise invert all bits: 0 => 18446744073709551615",
        sql: "SELECT ~0 AS test;",
        expected: Ok(select!("test" Value::I64; -1))
    };

    test! {
        name: "bit-wise invert all bits: 18446744073709551615 => 0",
        sql: "SELECT ~-1 AS test;",
        expected: Ok(select!("test" Value::I64; 0))
    };

    test! {
        name: "bit-wise invert all bits",
        sql: "SELECT ~(CAST(11936128518282651045 AS UINT64)) AS test;",
        expected: Ok(select!("test" Value::U64; 6510615555426900570))
    };

    test! {
        name: "bit-wise invert all bits",
        sql: "SELECT ~(CAST(6510615555426900570 AS UINT64)) AS test;",
        expected: Ok(select!("test" Value::U64; 11936128518282651045))
    };

    /* TODO: add failure cases; Is there any failure case for bit_not?? */
});
