use {crate::*, ast::DataType, std::borrow::Cow, uuid::Uuid as UUID};

test_case!(uuid, async move {
    use Value::*;

    let test_cases = vec![
        ("CREATE TABLE UUID (uuid_field UUID)", Ok(Payload::Create)),
        (
            r#"INSERT INTO UUID VALUES (0)"#,
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Uuid,
                literal: format!("{:?}", Literal::Number(Cow::Owned("0".to_owned()))),
            }
            .into()),
        ),
        (
            r#"INSERT INTO UUID VALUES (X'123')"#,
            Err(ValueError::FailedToParseUUID("123".to_string()).into()),
        ),
        (
            r#"INSERT INTO UUID VALUES ('NOT_UUID')"#,
            Err(ValueError::FailedToParseUUID("NOT_UUID".to_string()).into()),
        ),
        (
            r#"INSERT INTO UUID VALUES
            (X'936DA01F9ABD4d9d80C702AF85C822A8'),
            ('550e8400-e29b-41d4-a716-446655440000'),
            ('urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4')"#,
            Ok(Payload::Insert(3)),
        ),
        (
            r#"SELECT uuid_field AS uuid_field FROM UUID;"#,
            Ok(select!(
                uuid_field
                Uuid;
                UUID::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap().as_u128();
                UUID::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap().as_u128();
                UUID::parse_str("urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4").unwrap().as_u128()
            )),
        ),
        (
            r#"UPDATE UUID SET uuid_field = 'urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4' WHERE uuid_field='550e8400-e29b-41d4-a716-446655440000'"#,
            Ok(Payload::Update(1)),
        ),
        (
            r#"SELECT uuid_field AS uuid_field, COUNT(*) FROM UUID GROUP BY uuid_field"#,
            Ok(select!(
                uuid_field | "COUNT(*)"
                Uuid | I64;
                UUID::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap().as_u128()  1;
                UUID::parse_str("urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4").unwrap().as_u128()  2
            )),
        ),
        (
            r#"DELETE FROM UUID WHERE uuid_field='550e8400-e29b-41d4-a716-446655440000'"#,
            Ok(Payload::Delete(0)),
        ),
        (
            r#"DELETE FROM UUID WHERE uuid_field='urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4'"#,
            Ok(Payload::Delete(2)),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
