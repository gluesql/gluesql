use crate::{data::ValueError, prelude::Value::*, *};

test_case!(numeric, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT(8),
        field_two INT(8),
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_i8 = |text: &str| -> i8 { text.parse().unwrap() };

    test!(
        Err(ValueError::FailedToParseNumber.into()),
        "INSERT INTO Item VALUES (128, 128);"
    );
    test!(
        Err(ValueError::FailedToParseNumber.into()),
        "INSERT INTO Item VALUES (-129, -129);"
    );

    test!(
        Ok(select!(
            field_one        | field_two
            I8               |    I8;
            1                   parse_i8("-1");
            parse_i8("-2")         2;
            3                      3;
            parse_i8("-4")      parse_i8("-4")
        )),
        "SELECT field_one, field_two FROM Item"
    );

    test!(
        Ok(select!(
            field_one
            I8;
            1;
            3
        )),
        "SELECT field_one FROM Item WHERE field_one > 0"
    );
    test!(
        Ok(select!(
            field_one
            I8;
            1;
            3
        )),
        "SELECT field_one FROM Item WHERE field_one >= 0"
    );

    test!(
        Ok(select!(
            field_one
            I8;
            -2
        )),
        "SELECT field_one FROM Item WHERE field_one = -2"
    );

    test!(
        Ok(select!(
            field_one
            I8;
            -2;
            -4
        )),
        "SELECT field_one FROM Item WHERE field_one < 0"
    );

    test!(
        Ok(select!(
            field_one
            I8;
            -2;
            -4
        )),
        "SELECT field_one FROM Item WHERE field_one <= 0"
    );

    test!(
        Ok(select!(
            plus
            I8;
            0;
            0;
            6;
            -8
        )),
        "SELECT field_one + field_two AS plus FROM Item;"
    );

    test!(
        Ok(select!(
            sub
            I8;
            2;
            -4;
            0;
            0
        )),
        "SELECT field_one - field_two AS sub FROM Item;"
    );

    test!(
        Ok(select!(
            mul
            I8;
            -1;
            -4;
            9;
            16
        )),
        "SELECT field_one * field_two AS mul FROM Item;"
    );

    test!(
        Ok(select!(
            div
            I8;
            -1;
            -1;
            1;
            1
        )),
        "SELECT field_one / field_two AS div FROM Item;"
    );

    test!(
        Ok(select!(
            modulo
            I8;
            0;
            0;
            0;
            0
        )),
        "SELECT field_one % field_two AS modulo FROM Item;"
    );

    run!("DELETE FROM Item");
});
