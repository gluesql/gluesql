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

    test!(
        Err(ValueError::FailedToParseNumber.into()),
        &format!(
            "INSERT INTO Item VALUES ({:?}, {:?})",
            i64::MAX as i128 + 1,
            i64::MIN as i128 - 1
        )
    );

    // cast i64::MAX+1
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int,
            (i64::MAX as i128 + 1).to_string()
        )
        .into()),
        &format!("select cast({} as INT) from Item", i64::MAX as i128 + 1)
    );

    // cast i64::MIN-1
    test!(
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int,
            (i64::MIN as i128 - 1).to_string()
        )
        .into()),
        &format!("select cast({} as INT) from Item", i64::MIN as i128 - 1)
    );

    // lets try some valid SQL
    test!(
        Ok(select_with_comma!(
            field_one          | field_two
            I64                |    I64;
            1                  ,     -1;
            -2                 ,      2;
            3                  ,      3;
            -4                 ,     -4
        )),
        "SELECT field_one, field_two FROM Item"
    );

    test!(
        Ok(select!(field_one I64; 1)),
        "SELECT field_one FROM Item WHERE field_one = 1"
    );

    test!(
        Ok(select_with_comma!(field_one | field_two; I64 | I64; 1, -1)),
        "SELECT field_one, field_two FROM Item WHERE field_one = 1"
    );

    test!(
        Ok(select!(field_one I64; 1; 3)),
        "SELECT field_one FROM Item WHERE field_one >= 0"
    );

    test!(
        Ok(select!(field_one I64; -2)),
        "SELECT field_one FROM Item WHERE field_one = -2"
    );

    test!(
        Ok(select!(field_one I64; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one < 0"
    );

    test!(
        Ok(select!(field_one I64; -2; -4)),
        "SELECT field_one FROM Item WHERE field_one <= 0"
    );

    test!(
        Ok(select!(plus I64; 0; 0; 6; -8)),
        "SELECT field_one + field_two AS plus FROM Item;"
    );

    test!(
        Ok(select!(sub I64; 2; -4; 0; 0)),
        "SELECT field_one - field_two AS sub FROM Item;"
    );

    test!(
        Ok(select!(mul I64; -1; -4; 9; 16)),
        "SELECT field_one * field_two AS mul FROM Item;"
    );

    test!(
        Ok(select!(div I64; -1; -1; 1; 1)),
        "SELECT field_one / field_two AS div FROM Item;"
    );

    test!(
        Ok(select!(modulo I64; 0; 0; 0; 0)),
        "SELECT field_one % field_two AS modulo FROM Item;"
    );

    // try inserting i64 max and i64 min
    test!(
        Ok(Payload::Insert(1)),
        &format!("INSERT INTO Item VALUES ({}, {})", i64::MAX, i64::MIN)
    );

    run!("DELETE FROM Item");
});
