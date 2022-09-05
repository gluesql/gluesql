use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::*},
};

test_case!(int8, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT(8),
        field_two INT(8),
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_i8 = |text: &str| -> i8 { text.parse().unwrap() };

    test!(
        "INSERT INTO Item VALUES (128, 128);",
        Err(ValueError::FailedToParseNumber.into())
    );
    test!(
        "INSERT INTO Item VALUES (-129, -129);",
        Err(ValueError::FailedToParseNumber.into())
    );

    test!(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one        | field_two
            I8               |    I8;
            1                   parse_i8("-1");
            parse_i8("-2")         2;
            3                      3;
            parse_i8("-4")      parse_i8("-4")
        ))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I8; 1; 3))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I8; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I8; -2))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I8; -2; -4))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I8; -2; -4))
    );

    test!(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I8; 0; 0; 6; -8))
    );

    test!(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I8; 2; -4; 0; 0))
    );

    test!(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I8; -1; -4; 9; 16))
    );

    test!(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I8; -1; -1; 1; 1))
    );

    test!(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I8; 0; 0; 0; 0))
    );

    run!("DELETE FROM Item");
});
