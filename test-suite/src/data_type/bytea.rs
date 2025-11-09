use {
    crate::*,
    gluesql_core::{
        ast::DataType,
        error::{EvaluateError, LiteralError},
        prelude::{Payload, Value::Bytea},
    },
};

test_case!(bytea, {
    let g = get_tester!();

    let bytea = |v| hex::decode(v).unwrap();

    let test_cases = [
        ("CREATE TABLE Bytea (bytes BYTEA)", Ok(Payload::Create)),
        (
            "INSERT INTO Bytea VALUES
                (X'123456'),
                ('ab0123'),
                (X'936DA0');
            ",
            Ok(Payload::Insert(3)),
        ),
        (
            "SELECT * FROM Bytea",
            Ok(select!(
                bytes
                Bytea;
                bytea("123456");
                bytea("ab0123");
                bytea("936DA0")
            )),
        ),
        (
            "INSERT INTO Bytea VALUES (0)",
            Err(LiteralError::IncompatibleLiteralForDataType {
                data_type: DataType::Bytea,
                literal: "0".to_owned(),
            }
            .into()),
        ),
        (
            r"INSERT INTO Bytea VALUES (X'123')",
            Err(EvaluateError::FailedToDecodeHexString("123".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
