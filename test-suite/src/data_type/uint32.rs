use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(uint32, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
            field_one UINT32,
            field_two UINT32
        );",
    )
    .await;
    g.run(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#)
        .await;

    g.test(
        "INSERT INTO Item VALUES (4294967296,4294967296);",
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
            U32               | U32;
            1                   1;
            2                   2;
            3                   3;
            4                   4
        )),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U32; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U32; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U32; 2)),
    )
    .await;
});
