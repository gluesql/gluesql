use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::*},
};

test_case!(uint8, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT(8) UNSIGNED,
        field_two INT(8) UNSIGNED,
        );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_u8 = |text: &str| -> u8 { text.parse().unwrap() };

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
            U8               |    U8;
            1                   parse_u8("-1");
            parse_u8("-2")         2;
            3                      3;
            parse_u8("-4")      parse_u8("-4")
        ))
    );

});
