use {
    crate::*,
    gluesql_core::{data::NumericBinaryOperator, error::ValueError, prelude::Value::*},
};

test_case!(bitwise_shift_left, {
    let g = get_tester!();

    g.run(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER
)"#,
    )
    .await;

    g.run(
        r#"
CREATE TABLE OverflowTest (
    id INTEGER,
    num INTEGER
)"#,
    )
    .await;

    g.run(
        r#"
CREATE TABLE NullTest (
    id INTEGER,
    num INTEGER
)"#,
    )
    .await;

    g.run("INSERT INTO Test (id, num) VALUES (1, 1)").await;
    g.run("INSERT INTO Test (id, num) VALUES (1, 2)").await;
    g.run("INSERT INTO Test (id, num) VALUES (3, 4), (4, 8)")
        .await;

    g.run("INSERT INTO OverflowTest (id, num) VALUES (1, 1)")
        .await;

    g.run("INSERT INTO NullTest (id, num) VALUES (NULL, 1)")
        .await;

    g.named_test(
        "select all from table",
        "SELECT (num << 1) as num FROM Test",
        Ok(select!(num I64; 2; 4; 8; 16)),
    )
    .await;

    g.named_test(
        "test bit shift overflow",
        "SELECT (num << 65) as overflowed FROM OverflowTest",
        Err(ValueError::BinaryOperationOverflow {
            lhs: I64(1),
            rhs: U32(65),
            operator: NumericBinaryOperator::BitwiseShiftLeft,
        }
        .into()),
    )
    .await;

    g.test(
        "SELECT id, num FROM NullTest",
        Ok(select_with_null!(
            id     | num;
            Null     I64(1)
        )),
    )
    .await;
});
