use {
    crate::*,
    gluesql_core::{
        error::ValueError,
        prelude::{DataType, Payload, Value::*},
    },
};

test_case!(int32, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
        field_one INT32,
        field_two INT32
    );",
    )
    .await;
    g.run("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);")
        .await;

    let parse_i32 = |text: &str| -> i32 { text.parse().unwrap() };

    g.test(
        &format!(
            "INSERT INTO Item VALUES ({}, {i64})",
            i32::MAX as i64 + 1_i64,
            i64 = i32::MIN as i64 - 1_i64
        ),
        Err(ValueError::FailedToParseNumber.into()),
    )
    .await;

    g.test(
        &format!(
            "select cast({} as INT32) from Item",
            i32::MAX as i64 + 1_i64
        ),
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int32,
            (i32::MAX as i64 + 1_i64).to_string(),
        )
        .into()),
    )
    .await;

    g.test(
        &format!(
            "select cast({} as INT32) from Item",
            i32::MIN as i64 - 1_i64
        ),
        Err(ValueError::LiteralCastToDataTypeFailed(
            DataType::Int32,
            (i32::MIN as i64 - 1_i64).to_string(),
        )
        .into()),
    )
    .await;

    // lets try some valid SQL
    g.test(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one          | field_two
            I32                |    I32;
            1                  parse_i32("-1");
            parse_i32("-2")    2;
            3                  3;
            parse_i32("-4")    parse_i32("-4")
        )),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one = 1",
        Ok(select!(field_one I32; 1)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I32; 1; 3)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I32; 1; 3)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I32; -2)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I32; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I32; -2; -4)),
    )
    .await;

    g.test(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I32; 0; 0; 6; -8)),
    )
    .await;

    g.test(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I32; 2; -4; 0; 0)),
    )
    .await;

    g.test(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I32; -1; -4; 9; 16)),
    )
    .await;

    g.test(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I32; -1; -1; 1; 1)),
    )
    .await;

    g.test(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I32; 0; 0; 0; 0)),
    )
    .await;

    // try inserting i32 max and i32 min
    g.test(
        &format!("INSERT INTO Item VALUES ({}, {})", i32::MAX, i32::MIN),
        Ok(Payload::Insert(1)),
    )
    .await;

    g.run("DELETE FROM Item").await;
});
