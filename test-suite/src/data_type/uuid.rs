use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        ast::DataType,
        data::Literal,
        error::ValueError,
        prelude::{Payload, Value::*},
    },
    std::borrow::Cow,
    uuid::Uuid as UUID,
};

test_case!(uuid, {
    let g = get_tester!();

    let parse_uuid = |v| UUID::parse_str(v).unwrap().as_u128();

    {
        let uuid = UUID::now_v7();

        let test_cases = [
            (
                "CREATE TABLE posts (id UUID PRIMARY KEY)",
                Ok(Payload::Create),
            ),
            (
                &format!(r#"INSERT INTO posts ("id") VALUES ('{uuid}')"#),
                Ok(Payload::Insert(1)),
            ),
        ];

        for (sql, expected) in test_cases {
            g.test(sql, expected).await;
        }

        let glue = g.get_glue();
        let sql = format!("SELECT id FROM posts WHERE id = '{uuid}';", uuid = uuid);
        let payload = glue.execute(sql).await.unwrap();

        assert_eq!(payload, vec![select!( id Uuid; uuid.as_u128() )]);
    }

    let test_cases = [
        ("CREATE TABLE UUID (uuid_field UUID)", Ok(Payload::Create)),
        (
            r#"INSERT INTO UUID VALUES (0)"#,
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Uuid,
                literal: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(0)))),
            }
            .into()),
        ),
        (
            r#"INSERT INTO UUID VALUES (X'1234')"#,
            Err(ValueError::FailedToParseUUID("1234".to_owned()).into()),
        ),
        (
            r#"INSERT INTO UUID VALUES ('NOT_UUID')"#,
            Err(ValueError::FailedToParseUUID("NOT_UUID".to_owned()).into()),
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
                parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8");
                parse_uuid("550e8400-e29b-41d4-a716-446655440000");
                parse_uuid("urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4")
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
                parse_uuid("936DA01F9ABD4d9d80C702AF85C822A8")  1;
                parse_uuid("urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4")  2
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

    for (sql, expected) in test_cases {
        g.test(sql, expected).await;
    }
});
