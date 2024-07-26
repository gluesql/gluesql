use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(uint16, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
            field_one UINT16,
            field_two UINT16
        );",
    )
    .await;
    g.run(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#)
        .await;

    g.test(
        "INSERT INTO Item VALUES (327689,327689);",
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
            U16               | U16;
            1                   1;
            2                   2;
            3                   3;
            4                   4
        )),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U16; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U16; 1; 2;3;4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U16; 2)),
    )
    .await;
});
