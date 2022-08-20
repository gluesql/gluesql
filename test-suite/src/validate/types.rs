use crate::*;

test_case!(types, async move {
    use {
        gluesql_core::{
            ast::DataType,
            data::{Literal, ValueError},
            prelude::Value,
        },
        std::borrow::Cow,
    };

    run!("CREATE TABLE TableB (id BOOLEAN);");
    run!("CREATE TABLE TableC (uid INTEGER, null_val INTEGER NULL);");
    run!("INSERT INTO TableB VALUES (FALSE);");
    run!("INSERT INTO TableC VALUES (1, NULL);");

    let test_cases = [
        (
            "INSERT INTO TableB SELECT uid FROM TableC;",
            Err(ValueError::IncompatibleDataType {
                data_type: DataType::Boolean,
                value: Value::I64(1),
            }
            .into()),
        ),
        (
            "INSERT INTO TableC (uid) VALUES (\"A\")",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Int,
                literal: format!("{:?}", Literal::Text(Cow::Owned("A".to_owned()))),
            }
            .into()),
        ),
        (
            "INSERT INTO TableC VALUES (NULL, 30);",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
        (
            "INSERT INTO TableC SELECT null_val FROM TableC;",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
        (
            "UPDATE TableC SET uid = TRUE;",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Int,
                literal: format!("{:?}", Literal::Boolean(true)),
            }
            .into()),
        ),
        (
            "UPDATE TableC SET uid = (SELECT id FROM TableB LIMIT 1) WHERE uid = 1",
            Err(ValueError::IncompatibleDataType {
                data_type: DataType::Int,
                value: Value::Bool(false),
            }
            .into()),
        ),
        (
            "UPDATE TableC SET uid = NULL;",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
        (
            "UPDATE TableC SET uid = (SELECT null_val FROM TableC);",
            Err(ValueError::NullValueOnNotNullField.into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
