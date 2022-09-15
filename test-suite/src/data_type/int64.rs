use {
    crate::*,
    gluesql_core::{
        data::ValueError,
        prelude::{DataType, Payload, Value::*},
    },
};

test_case!(int64, async move {
    run!(
        "CREATE TABLE Item (
        field_one INT,
        field_two INT,
    );"
    );
    run!("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_i64 = |text: &str| -> i64 { text.parse().unwrap() };

    test!(
        &format!(
            "INSERT INTO Item VALUES ({:?}, {:?})",
            i64::MAX as i128 + 1,
            i64::MIN as i128 - 1
        ),
        Err(ValueError::FailedToParseNumber.into())
    );

    // cast i64::MAX+1
    test!(
        &format!("select cast({} as INT) from Item", i64::MAX as i128 + 1),
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int,
            (i64::MAX as i128 + 1).to_string()
        )
        .into())
    );

    // cast i64::MIN-1
    test!(
        &format!("select cast({} as INT) from Item", i64::MIN as i128 - 1),
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int,
            (i64::MIN as i128 - 1).to_string()
        )
        .into())
    );

    // lets try some valid SQL
    test!(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one          | field_two
            I64                |    I64;
            1                  parse_i64("-1");
            parse_i64("-2")    2;
            3                  3;
            parse_i64("-4")    parse_i64("-4")
        ))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = 1",
        Ok(select!(field_one I64; 1))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I64; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I64; 1; 3))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I64; -2))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I64; -2; -4))
    );

    test!(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I64; -2; -4))
    );

    test!(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I64; 0; 0; 6; -8))
    );

    test!(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I64; 2; -4; 0; 0))
    );

    test!(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I64; -1; -4; 9; 16))
    );

    test!(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I64; -1; -1; 1; 1))
    );

    test!(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I64; 0; 0; 0; 0))
    );

    // try inserting i64 max and i64 min
    test!(
        &format!("INSERT INTO Item VALUES ({}, {})", i64::MAX, i64::MIN),
        Ok(Payload::Insert(1))
    );

    run!("DELETE FROM Item");
});
