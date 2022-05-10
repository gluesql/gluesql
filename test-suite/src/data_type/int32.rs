use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::DataType, prelude::Value::*},
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

    let max_str = "2147483648";
    let min_str = "-2147483649";

    // try to insert i32::MAX +1
    let str = format!("INSERT INTO Item VALUES ({:}, {:})", max_str, max_str);
    test!(Err(ValueError::FailedToParseNumber.into()), &str);

    // try to insert i32::MIN-1
    let str = format!("INSERT INTO Item VALUES ({:}, {:})", min_str, min_str);
    test!(Err(ValueError::FailedToParseNumber.into()), &str);

    // cast i128::MAX+1
    let str = format!("select cast({:} as INT(32)) from Item", max_str);
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int32, max_str.to_string()).into()),
        &str
    );

    //cast i128::MIN-1
    let str = format!("select cast({:} as INT(32)) from Item", min_str);
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int32, min_str.to_string()).into()),
        &str
    );

    // now try some valid sql statements
    test!(
        Ok(select!(
            field_one        | field_two
            I32               |    I32;
            1                   parse_i32("-1");
            parse_i32("-2")         2;
            3                      3;
            parse_i32("-4")      parse_i32("-4")
        )),
        "SELECT field_one, field_two FROM Item"
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

    run!("DELETE FROM Item");
});
