use crate::{data::ValueError, prelude::Value::*, *};

test_case!(numeric, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT(8),
        field_two INT(8),
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (0, 127), (0, -127);");

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
            0                     127;
            0                   parse_i8("-127")
        )),
        "SELECT field_one, field_two FROM Item"
    );

    test!(
        Ok(select!(
            field_one
            I8;
            1
        )),
        "SELECT field_one FROM Item WHERE field_one > 0"
    );
    test!(
        Ok(select!(
            field_one
            I8;
            1;
            0;
            0
        )),
        "SELECT field_one FROM Item WHERE field_one >= 0"
    );

    test!(
        Ok(select!(
            field_one
            I8;
            0;
            0
        )),
        "SELECT field_one FROM Item WHERE field_one = 0"
    );

    test!(
        Ok(select!(
            field_one
            I8;
            -2
        )),
        "SELECT field_one FROM Item WHERE field_one < 0"
    );
    test!(
        Ok(select!(
            field_one
            I8;
            -2;
            0;
            0
        )),
        "SELECT field_one FROM Item WHERE field_one <= 0"
    );

    test!(
        Ok(select!(
            plus             |      sub           |       mul      |       div       |   modulo
            I8               |      I8            |        I8      |       I8        |     I8;
            0                        2              parse_i8("-1")    parse_i8("-1")        0;
            0                   parse_i8("-4")      parse_i8("-4")    parse_i8("-1")        0;
            127                 parse_i8("-127")           0                0               0;
            parse_i8("-127")        127                    0                0               0
        )),
        "SELECT
            field_one + field_two AS plus,
            field_one - field_two AS sub,
            field_one * field_two AS mul,
            field_one / field_two AS div,
            field_one % field_two AS modulo
            FROM Item;"
    );

    run!("DELETE FROM Item");
});
