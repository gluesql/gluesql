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
        Err(ValueError::FailedToParseNumber.into()),
        &format!(
            "INSERT INTO Item VALUES ({:?}, {:?})",
            i32::MAX as i64 + 1_i64,
            i64 = i32::MIN as i64 - 1_i64
        )
    );

    // cast i32::MAX+1
    // this should produce an error! will create a different PR / issue for fixing this.
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int32,
            (i32::MAX as i64 + 1_i64).to_string()
        )
        .into()),
        &format!(
            "select cast({} as INT(32)) from Item",
            i32::MAX as i64 + 1_i64
        )
    );

    // cast i32::MIN-1
    // this also should produce an error, will create a PR for it as well.
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int32,
            (i32::MIN as i64 - 1_i64).to_string()
        )
        .into()),
        &format!(
            "select cast({} as INT(32)) from Item",
            i32::MIN as i64 - 1_i64
        )
    );

    // lets try some valid SQL
    test!(
        Ok(select!(
            field_one          | field_two
            I32                |    I32;
            1                  parse_i32("-1");
            parse_i32("-2")    2;
            3                  3;
            parse_i32("-4")    parse_i32("-4")
        )),
        "SELECT field_one, field_two FROM Item"
    );

    test!(
        Ok(select!(field_one I32; 1)),
        "SELECT field_one FROM Item WHERE field_one = 1"
    );

    test!(
        Ok(select!(field_one I32; 1; 3)),
        "SELECT field_one FROM Item WHERE field_one > 0"
    );

    test!(
        Ok(select!(field_one I32; 1; 3)),
        "SELECT field_one FROM Item WHERE field_one >= 0"
    );

    test!(
        Ok(select!(field_one I32; -2)),
        "SELECT field_one FROM Item WHERE field_one = -2"
    );

    test!(
        Ok(select!(field_one I32; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one < 0"
    );

    test!(
        Ok(select!(field_one I32; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one <= 0"
    );

    test!(
        Ok(select!(plus I32; 0; 0; 6; -8)),
        "SELECT field_one + field_two AS plus FROM Item;"
    );

    test!(
        Ok(select!(sub I32; 2; -4; 0; 0)),
        "SELECT field_one - field_two AS sub FROM Item;"
    );

    test!(
        Ok(select!(mul I32; -1; -4; 9; 16)),
        "SELECT field_one * field_two AS mul FROM Item;"
    );

    test!(
        Ok(select!(div I32; -1; -1; 1; 1)),
        "SELECT field_one / field_two AS div FROM Item;"
    );

    test!(
        Ok(select!(modulo I32; 0; 0; 0; 0)),
        "SELECT field_one % field_two AS modulo FROM Item;"
    );

    // try inserting i32 max and i32 min
    test!(
        Ok(Payload::Insert(1)),
        &format!("INSERT INTO Item VALUES ({}, {})", i32::MAX, i32::MIN)
    );

    run!("DELETE FROM Item");
});
