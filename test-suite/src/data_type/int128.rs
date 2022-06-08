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

    let parse_i128 = |text: &str| -> i128 { text.parse().unwrap() };

    let max_str = "170141183460469231731687303715884105728";
    let min_str = "-170141183460469231731687303715884105729";

    let s = format!("INSERT INTO Item VALUES ({:}, {:})", max_str, max_str);

    test!(Err(ValueError::FailedToParseNumber.into()), &s);

    let s = format!("INSERT INTO Item VALUES ({:}, {:})", min_str, min_str);
    test!(Err(ValueError::FailedToParseNumber.into()), &s);

    // cast i128::MAX+1
    let s = format!("select cast({:} as INT(128)) from Item", max_str);
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, max_str.to_string()).into()),
        &s
    );

    // cast i128::MIN-1
    let s = format!("select cast({:} as INT(128)) from Item", min_str);
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, min_str.to_string()).into()),
        &s
    );

    // lets try some valid SQL
    test!(
        Ok(select!(
            field_one        | field_two
            I128               |    I128;
            1                   parse_i128("-1");
            parse_i128("-2")        2;
            3                      3;
            parse_i128("-4")      parse_i128("-4")
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
