use {
    crate::*,
    gluesql_core::{data::NumericBinaryOperator, error::ValueError, prelude::Value::*},
};

test_case!(basic, async move {
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

    run!("INSERT INTO NullTest (id, num) VALUES (1, NULL)");
    run!("INSERT INTO NullTest (id, num) VALUES (NULL, 1)");
    run!("INSERT INTO NullTest (id, num) VALUES (NULL, NULL)");

    test! (
        name: "select all from table",
        sql : "SELECT (num << 1) as num FROM Test",
        expected : Ok(select!(num I64; 2; 4; 8; 16))
    );

    test!(
        name : "test bit shift overflow",
        sql : "SELECT (num << 65) as overflowed FROM Test",
        expected : Err(ValueError::BinaryOperationOverflow { lhs : U8(1), rhs : U8(65), operator : NumericBinaryOperator::ShiftLeft}.into())
    );

    test!(
        "SELECT id, num FROM NullTest",
        Ok(select_with_null!(
            id     | num;
            I64(1)   Null;
            Null     I64(1);
            Null     Null
        ))
    );
});
