use {
    crate::*,
    gluesql_core::{data::NumericBinaryOperator, error::ValueError, prelude::Value::*},
};

test_case!(bit_shift_left, async move {
    run!(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
)"#
    );

    run!(
        r#"
CREATE TABLE OverflowTest (
    id INTEGER,
    num INTEGER,
)"#
    );

    run!(
        r#"
CREATE TABLE NullTest (
    id INTEGER,
    num INTEGER,
)"#
    );

    run!("INSERT INTO Test (id, num) VALUES (1, 1)");
    run!("INSERT INTO Test (id, num) VALUES (1, 2)");
    run!("INSERT INTO Test (id, num) VALUES (3, 4), (4, 8)");

    run!("INSERT INTO OverflowTest (id, num) VALUES (1, 1)");

    run!("INSERT INTO NullTest (id, num) VALUES (NULL, 1)");

    test! (
        name: "select all from table",
        sql : "SELECT (num << 1) as num FROM Test",
        expected : Ok(select!(num I64; 2; 4; 8; 16))
    );

    test!(
        name : "test bit shift overflow",
        sql : "SELECT (num << 65) as overflowed FROM OverflowTest",
        expected : Err(ValueError::BinaryOperationOverflow { lhs : I64(1), rhs : I64(65), operator : NumericBinaryOperator::BitwiseShiftLeft}.into())
    );

    test!(
        "SELECT id, num FROM NullTest",
        Ok(select_with_null!(
            id     | num;
            Null     I64(1)
        ))
    );
});
