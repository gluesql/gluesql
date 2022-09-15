use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::*},
};

test_case!(uint16, async move {
    run!(
        "CREATE TABLE Item (
            field_one INT(16) UNSIGNED,
            field_two INT(16) UNSIGNED,
        );"
    );
    run!(r#"INSERT INTO Item VALUES (1, 2), (1, 3), (2, 4), (2, 5);"#);

    let parse_u16 = |text: &str| -> u16 { text.parse().unwrap() };

    test!(
        "INSERT INTO Item VALUES (32768,32768);",
        Err(ValueError::FailedToParseNumber.into())
    );

    test!(
        "INSERT INTO Item VALUES (-32769, -32769);",
        Err(ValueError::FailedToParseNumber.into())
    );
    test!(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one         | field_two
            U16               | U16;
            1                   parse_u16("-1");
            parse_u16("-2")     2;
            3                   3;
            parse_u16("-4")     parse_u16("-4")
        ))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U16; 1; 3))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U16; 1; 3))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U16; 2))
    );
});
