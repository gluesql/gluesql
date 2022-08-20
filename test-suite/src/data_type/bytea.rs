use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        ast::DataType,
        data::{Literal, LiteralError, ValueError},
        executor::Payload,
        prelude::Value::Bytea,
    },
    std::borrow::Cow,
};

test_case!(bytea, async move {
    let bytea = |v| hex::decode(v).unwrap();

    let test_cases = [
        ("CREATE TABLE Bytea (key BYTEA)", Ok(Payload::Create)),
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
                key
                Bytea;
                bytea("123456");
                bytea("ab0123");
                bytea("936DA0")
            )),
        ),
        (
            "INSERT INTO Bytea VALUES (0)",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Bytea,
                literal: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(0)))),
            }
            .into()),
        ),
        (
            r#"INSERT INTO Bytea VALUES (X'123')"#,
            Err(LiteralError::FailedToDecodeHexString("123".to_string()).into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
