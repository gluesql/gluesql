use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(int16, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
        field_one INT16,
        field_two INT16
    );",
    )
    .await;
    g.run("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);")
        .await;

    let parse_i16 = |text: &str| -> i16 { text.parse().unwrap() };

    g.test(
        "INSERT INTO Item VALUES (32768, 32768);",
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
            field_one        |  field_two
            I16              |  I16;
            1                   parse_i16("-1");
            parse_i16("-2")     2;
            3                   3;
            parse_i16("-4")     parse_i16("-4")
        )),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I16; 1; 3)),
    )
    .await;
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I16; 1; 3)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I16; -2)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I16; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I16; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I16; 0; 0; 6; -8)),
    )
    .await;

    g.test(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I16; 2; -4; 0; 0)),
    )
    .await;

    g.test(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I16; -1; -4; 9; 16)),
    )
    .await;

    g.test(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I16; -1; -1; 1; 1)),
    )
    .await;

    g.test(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I16; 0; 0; 0; 0)),
    )
    .await;

    g.run("DELETE FROM Item").await;
});
