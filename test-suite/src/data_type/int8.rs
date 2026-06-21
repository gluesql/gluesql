use {
    crate::*,
    gluesql_core::{
        error::EvaluateError,
        prelude::{DataType, Value::*},
    },
};

test_case!(int8, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
        field_one INT8,
        field_two INT8
    );",
    );
    g.run("INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);");

    let parse_i8 = |text: &str| -> i8 { text.parse().unwrap() };

    g.test(
        "INSERT INTO Item VALUES (128, 128);",
        Err(EvaluateError::NumberParseFailed {
            literal: "128".to_owned(),
            data_type: DataType::Int8,
        }
        .into()),
    );
    g.test(
        "INSERT INTO Item VALUES (-129, -129);",
        Err(EvaluateError::NumberParseFailed {
            literal: "-129".to_owned(),
            data_type: DataType::Int8,
        }
        .into()),
    );

    g.test(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one        | field_two
            I8               |    I8;
            1                   parse_i8("-1");
            parse_i8("-2")         2;
            3                      3;
            parse_i8("-4")      parse_i8("-4")
        )),
    );

    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one I8; 1; 3)),
    );
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one I8; 1; 3)),
    );

    g.test(
        "SELECT field_one FROM Item WHERE field_one = -2",
        Ok(select!(field_one I8; -2)),
    );

    g.test(
        "SELECT field_one FROM Item WHERE field_one < 0",
        Ok(select!(field_one I8; -2; -4)),
    );

    g.test(
        "SELECT field_one FROM Item WHERE field_one <= 0",
        Ok(select!(field_one I8; -2; -4)),
    );

    g.test(
        "SELECT field_one + field_two AS plus FROM Item;",
        Ok(select!(plus I8; 0; 0; 6; -8)),
    );

    g.test(
        "SELECT field_one - field_two AS sub FROM Item;",
        Ok(select!(sub I8; 2; -4; 0; 0)),
    );

    g.test(
        "SELECT field_one * field_two AS mul FROM Item;",
        Ok(select!(mul I8; -1; -4; 9; 16)),
    );

    g.test(
        "SELECT field_one / field_two AS div FROM Item;",
        Ok(select!(div I8; -1; -1; 1; 1)),
    );

    g.test(
        "SELECT field_one % field_two AS modulo FROM Item;",
        Ok(select!(modulo I8; 0; 0; 0; 0)),
    );

    g.run("DELETE FROM Item");
});
