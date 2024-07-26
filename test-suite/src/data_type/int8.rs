use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(int8, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
        field_one INT8,
        field_two INT8
    );",
    )
    .await;
    g.run("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);")
        .await;

    let parse_i8 = |text: &str| -> i8 { text.parse().unwrap() };

    g.test(
        "INSERT INTO Item VALUES (128, 128);",
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
            I8               |    I8;
            1                   parse_i8("-1");
            parse_i8("-2")         2;
            3                      3;
            parse_i8("-4")      parse_i8("-4")
        )),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I8; 1; 3)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I8; 1; 3)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I8; -2)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I8; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I8; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I8; 0; 0; 6; -8)),
    )
    .await;

    g.test(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I8; 2; -4; 0; 0)),
    )
    .await;

    g.test(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I8; -1; -4; 9; 16)),
    )
    .await;

    g.test(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I8; -1; -1; 1; 1)),
    )
    .await;

    g.test(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I8; 0; 0; 0; 0)),
    )
    .await;

    g.run("DELETE FROM Item").await;
});
