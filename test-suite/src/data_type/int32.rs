use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::DataType, prelude::Payload, prelude::Value::*},
};

test_case!(int32, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT(32),
        field_two INT(32),
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_i32 = |text: &str| -> i32 { text.parse().unwrap() };

    test!(
        &format!(
            "INSERT INTO Item VALUES ({}, {i64})",
            i32::MAX as i64 + 1_i64,
            i64 = i32::MIN as i64 - 1_i64
        ),
        Err(ValueError::FailedToParseNumber.into())
    );

    test!(
        &format!(
            "select cast({} as INT(32)) from Item",
            i32::MAX as i64 + 1_i64
        ),
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int32,
            (i32::MAX as i64 + 1_i64).to_string()
        )
        .into())
    );

    test!(
        &format!(
            "select cast({} as INT(32)) from Item",
            i32::MIN as i64 - 1_i64
        ),
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int32,
            (i32::MIN as i64 - 1_i64).to_string()
        )
        .into())
    );

    // lets try some valid SQL
    test!(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one          | field_two
            I32                |    I32;
            1                  parse_i32("-1");
            parse_i32("-2")    2;
            3                  3;
            parse_i32("-4")    parse_i32("-4")
        ))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = 1",
        Ok(select!(field_one I32; 1))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I32; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I32; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I32; -2))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I32; -2; -4))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I32; -2; -4))
    );

    test!(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I32; 0; 0; 6; -8))
    );

    test!(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I32; 2; -4; 0; 0))
    );

    test!(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I32; -1; -4; 9; 16))
    );

    test!(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I32; -1; -1; 1; 1))
    );

    test!(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I32; 0; 0; 0; 0))
    );

    // try inserting i32 max and i32 min
    test!(
        &format!("INSERT INTO Item VALUES ({}, {})", i32::MAX, i32::MIN),
        Ok(Payload::Insert(1))
    );

    run!("DELETE FROM Item");
});
