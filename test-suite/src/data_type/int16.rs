use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::*},
};

test_case!(int16, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT(16),
        field_two INT(16),
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_i16 = |text: &str| -> i16 { text.parse().unwrap() };

    test!(
        "INSERT INTO Item VALUES (32768, 32768);",
        Err(ValueError::FailedToParseNumber.into())
    );
    test!(
        "INSERT INTO Item VALUES (-32769, -32769);",
        Err(ValueError::FailedToParseNumber.into())
    );

    test!(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one        |  field_two
            I16              |  I16;
            1                   parse_i16("-1");
            parse_i16("-2")     2;
            3                   3;
            parse_i16("-4")     parse_i16("-4")
        ))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I16; 1; 3))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I16; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I16; -2))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I16; -2; -4))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I16; -2; -4))
    );

    test!(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I16; 0; 0; 6; -8))
    );

    test!(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I16; 2; -4; 0; 0))
    );

    test!(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I16; -1; -4; 9; 16))
    );

    test!(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I16; -1; -1; 1; 1))
    );

    test!(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I16; 0; 0; 0; 0))
    );

    run!("DELETE FROM Item");
});
