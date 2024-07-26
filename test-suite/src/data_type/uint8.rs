use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(uint8, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
            field_one UINT8,
            field_two UINT8
        );",
    )
    .await;
    g.run(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#)
        .await;

    g.test(
        "INSERT INTO Item VALUES (256, 256);",
        Err(ValueError::FailedToParseNumber.into()),
    )
    .await;

    g.test(
        "INSERT INTO Item VALUES (-129, -129);",
        Err(ValueError::FailedToParseNumber.into()),
    )
    .await;
    g.test(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one        | field_two
            U8               |    U8;
            1                      1;
            2                      2;
            3                      3;
            4                      4
        )),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U8; 1; 2; 3; 4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U8; 1; 2; 3; 4)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U8; 2)),
    )
    .await;
});
