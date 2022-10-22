use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::Value::*},
};

test_case!(uint8, async move {
    run!(
        "CREATE TABLE Item (
            field_one UINT8,
            field_two UINT8,
        );"
    );
    run!(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#);

    test!(
        "INSERT INTO Item VALUES (256, 256);",
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
            1                      1;
            2                      2;
            3                      3;
            4                      4
        ))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U8; 1; 2; 3; 4))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U8; 1; 2; 3; 4))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U8; 2))
    );
});
