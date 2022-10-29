use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::*},
};

test_case!(uint16, async move {
    run!(
        "CREATE TABLE Item (
            field_one UINT16,
            field_two UINT16,
        );"
    );
    run!(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#);

    test!(
        "INSERT INTO Item VALUES (327689,327689);",
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
            1                   1;
            2                   2;
            3                   3;
            4                   4
        ))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U16; 1; 2;3;4))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U16; 1; 2;3;4))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U16; 2))
    );
});
