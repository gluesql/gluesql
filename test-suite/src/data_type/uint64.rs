use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(uint64, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
            field_one UINT64,
            field_two UINT64
        );",
    )
    .await;
    g.run(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#)
        .await;

    g.test(
        "INSERT INTO Item VALUES (18446744073709551616,18446744073709551616);",
        Err(ValueError::FailedToParseNumber.into()),
    )
    .await;

    g.test(
        "INSERT INTO Item VALUES (-32769, -32769);",
        Err(ValueError::FailedToParseNumber.into()),
    )
    .await;
    g.test(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one         | field_two
            U64               | U64;
            1                   1;
            2                   2;
            3                   3;
            4                   4
        )),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U64; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U64; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U64; 2)),
    )
    .await;
});
