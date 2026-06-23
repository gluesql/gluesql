use {
    crate::*,
    gluesql_core::{
        error::EvaluateError,
        prelude::{DataType, Value::*},
    },
};

test_case!(uint8, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Item (
            field_one UINT8,
            field_two UINT8
        );",
    );
    g.run(r"INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);");

    g.test(
        "INSERT INTO Item VALUES (256, 256);",
        Err(EvaluateError::NumberParseFailed {
            literal: "256".to_owned(),
            data_type: DataType::Uint8,
        }
        .into()),
    );

    g.test(
        "INSERT INTO Item VALUES (-129, -129);",
        Err(EvaluateError::NumberParseFailed {
            literal: "-129".to_owned(),
            data_type: DataType::Uint8,
        }
        .into()),
    );
    g.test(
        "SELECT field_one, field_two FROM Item",
        Ok(select!(
            field_one        | field_two
            U8               |    U8;
            1                      1;
            2                      2;
            3                      3;
            4                      4
        )),
    );
    g.test(
        "SELECT field_one FROM Item WHERE field_one > 0",
        Ok(select!(field_one U8; 1; 2; 3; 4)),
    );
    g.test(
        "SELECT field_one FROM Item WHERE field_one >= 0",
        Ok(select!(field_one U8; 1; 2; 3; 4)),
    );
    g.test(
        "SELECT field_one FROM Item WHERE field_one = 2",
        Ok(select!(field_one U8; 2)),
    );
});
