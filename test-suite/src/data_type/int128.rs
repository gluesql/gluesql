use {
    crate::*,
    gluesql_core::{data::ValueError, prelude::DataType, prelude::Value::*},
};

test_case!(int128, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT128,
        field_two INT128,
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_i128 = |text: &str| -> i128 { text.parse().unwrap() };

    let max_str = "170141183460469231731687303715884105728";
    let min_str = "-170141183460469231731687303715884105729";

    test!(
        &format!("INSERT INTO Item VALUES ({}, {})", max_str, max_str),
        Err(ValueError::FailedToParseNumber.into())
    );

    // cast i128::MAX+1
    test!(
        &format!("select cast({} as INT128) from Item", max_str),
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, max_str.to_string()).into())
    );

    // cast i128::MIN-1
    test!(
        &format!("select cast({} as INT128) from Item", min_str),
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, min_str.to_string()).into())
    );

    // lets try some valid SQL
    test!(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one          | field_two
            I128               |  I128;
            1                    parse_i128("-1");
            parse_i128("-2")     2;
            3                    3;
            parse_i128("-4")     parse_i128("-4")
        ))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = 1",
        Ok(select!(field_one I128; 1))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I128; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I128; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I128; -2))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I128; -2; -4))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I128; -2; -4))
    );

    test!(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I128; 0; 0; 6; -8))
    );

    test!(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I128; 2; -4; 0; 0))
    );

    test!(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I128; -1; -4; 9; 16))
    );

    test!(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I128; -1; -1; 1; 1))
    );

    test!(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I128; 0; 0; 0; 0))
    );

    run!("DELETE FROM Item");
});
