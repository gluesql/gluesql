use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::DataType, prelude::Value::*},
};

test_case!(int128, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT(128),
        field_two INT(128),
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let max_str = "170141183460469231731687303715884105728";
    let min_str = "-170141183460469231731687303715884105729";

    test!(
        Err(ValueError::FailedToParseNumber.into()),
        &format!("INSERT INTO Item VALUES ({}, {})", max_str, max_str)
    );

    // cast i128::MAX+1
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, max_str.to_string()).into()),
        &format!("select cast({} as INT(128)) from Item", max_str)
    );

    // cast i128::MIN-1
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, min_str.to_string()).into()),
        &format!("select cast({} as INT(128)) from Item", min_str)
    );

    // lets try some valid SQL
    test!(
        Ok(select_with_comma!(
            field_one          | field_two
            I128               |  I128;
            1                  ,    -1;
            -2                 ,     2;
            3                  ,     3;
            -4                 ,    -4
        )),
        "SELECT field_one, field_two FROM Item"
    );

    test!(
        Ok(select!(field_one I128; 1)),
        "SELECT field_one FROM Item WHERE field_one = 1"
    );

    test!(
        Ok(select!(field_one I128; 1; 3)),
        "SELECT field_one FROM Item WHERE field_one > 0"
    );

    test!(
        Ok(select!(field_one I128; 1; 3)),
        "SELECT field_one FROM Item WHERE field_one >= 0"
    );

    test!(
        Ok(select!(field_one I128; -2)),
        "SELECT field_one FROM Item WHERE field_one = -2"
    );

    test!(
        Ok(select!(field_one I128; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one < 0"
    );

    test!(
        Ok(select!(field_one I128; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one <= 0"
    );

    test!(
        Ok(select!(plus I128; 0; 0; 6; -8)),
        "SELECT field_one + field_two AS plus FROM Item;"
    );

    test!(
        Ok(select!(sub I128; 2; -4; 0; 0)),
        "SELECT field_one - field_two AS sub FROM Item;"
    );

    test!(
        Ok(select!(mul I128; -1; -4; 9; 16)),
        "SELECT field_one * field_two AS mul FROM Item;"
    );

    test!(
        Ok(select!(div I128; -1; -1; 1; 1)),
        "SELECT field_one / field_two AS div FROM Item;"
    );

    test!(
        Ok(select!(modulo I128; 0; 0; 0; 0)),
        "SELECT field_one % field_two AS modulo FROM Item;"
    );

    run!("DELETE FROM Item");
});
