use {
    crate::*,
    gluesql_core::{error::ValueError, prelude::Value::*},
};

test_case!(uint64, async move {
    run!(
        "CREATE TABLE Item (
            field_one UINT64,
            field_two UINT64,
        );"
    );
    run!(r#"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);"#);

    test!(
        "INSERT INTO Item VALUES (18446744073709551616,18446744073709551616);",
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
            U64               | U64;
            1                   1;
            2                   2;
            3                   3;
            4                   4
        ))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U64; 1; 2;3;4))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U64; 1; 2;3;4))
    );
    test!(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U64; 2))
    );
});
