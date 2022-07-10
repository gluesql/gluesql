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
        Err(ValueError::FailedToParseNumber.into()),
        "INSERT INTO Item VALUES (32768, 32768);"
    );
    test!(
        Err(ValueError::FailedToParseNumber.into()),
        "INSERT INTO Item VALUES (-32769, -32769);"
    );

    test!(
        Ok(select!(
            field_one        |  field_two
            I16              |  I16;
            1                   parse_i16("-1");
            parse_i16("-2")     2;
            3                   3;
            parse_i16("-4")     parse_i16("-4")
        )),
        "SELECT field_one, field_two FROM Item"
    );

    test!(
        Ok(select!(field_one I16; 1; 3)),
        "SELECT field_one FROM Item WHERE field_one > 0"
    );
    test!(
        Ok(select!(field_one I16; 1; 3)),
        "SELECT field_one FROM Item WHERE field_one >= 0"
    );

    test!(
        Ok(select!(field_one I16; -2)),
        "SELECT field_one FROM Item WHERE field_one = -2"
    );

    test!(
        Ok(select!(field_one I16; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one < 0"
    );

    test!(
        Ok(select!(field_one I16; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one <= 0"
    );

    // test!(
    //     Ok(select!(plus I16; 0; 0; 6; -8)),
    //     "SELECT field_one + field_two AS plus FROM Item;"
    // );

    test!(
        Ok(select!(sub I16; 2; -4; 0; 0)),
        "SELECT field_one - field_two AS sub FROM Item;"
    );

    test!(
        Ok(select!(mul I16; -1; -4; 9; 16)),
        "SELECT field_one * field_two AS mul FROM Item;"
    );

    test!(
        Ok(select!(div I16; -1; -1; 1; 1)),
        "SELECT field_one / field_two AS div FROM Item;"
    );

    test!(
        Ok(select!(modulo I16; 0; 0; 0; 0)),
        "SELECT field_one % field_two AS modulo FROM Item;"
    );

    run!("DELETE FROM Item");
});
