use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(uint128, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
            field_one UINT128,
            field_two UINT128
        );",
    )
    .await;
    g.run(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#)
        .await;

    g.test(
        "INSERT INTO Item VALUES (340282366920938463463374607431768211456,340282366920938463463374607431768211456);",
        Err(ValueError::FailedToParseNumber.into())
    ).await;

    g.test(
        "INSERT INTO Item VALUES (-32769, -32769);",
        Err(ValueError::FailedToParseNumber.into()),
    )
    .await;
    g.test(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one         | field_two
            U128              | U128;
            1                   1;
            2                   2;
            3                   3;
            4                   4
        )),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U128; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U128; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U128; 2)),
    )
    .await;
});
