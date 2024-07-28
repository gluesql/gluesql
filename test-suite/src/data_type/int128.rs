use {
    crate::*,
    gluesql_core::{
        error::ValueError,
        prelude::{DataType, Value::*},
    },
};

test_case!(int128, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
        field_one INT128,
        field_two INT128
    );",
    )
    .await;
    g.run("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);")
        .await;

    let parse_i128 = |text: &str| -> i128 { text.parse().unwrap() };

    let max_str = "170141183460469231731687303715884105728";
    let min_str = "-170141183460469231731687303715884105729";

    g.test(
        &format!("INSERT INTO Item VALUES ({}, {})", max_str, max_str),
        Err(ValueError::FailedToParseNumber.into()),
    )
    .await;

    // cast i128::MAX+1
    g.test(
        &format!("select cast({} as INT128) from Item", max_str),
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, max_str.to_owned()).into()),
    )
    .await;

    // cast i128::MIN-1
    g.test(
        &format!("select cast({} as INT128) from Item", min_str),
        Err(ValueError::LiteralCastToDataTypeFailed(DataType::Int128, min_str.to_owned()).into()),
    )
    .await;

    // lets try some valid SQL
    g.test(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one          | field_two
            I128               |  I128;
            1                    parse_i128("-1");
            parse_i128("-2")     2;
            3                    3;
            parse_i128("-4")     parse_i128("-4")
        )),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one = 1",
        Ok(select!(field_one I128; 1)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I128; 1; 3)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I128; 1; 3)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I128; -2)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I128; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I128; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I128; 0; 0; 6; -8)),
    )
    .await;

    g.test(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I128; 2; -4; 0; 0)),
    )
    .await;

    g.test(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I128; -1; -4; 9; 16)),
    )
    .await;

    g.test(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I128; -1; -1; 1; 1)),
    )
    .await;

    g.test(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I128; 0; 0; 0; 0)),
    )
    .await;

    g.run("DELETE FROM Item").await;
});
