use {
    crate::*,
    bigdecimal::BigDecimal,
    gluesql_core::{
        ast::DataType::{Boolean, Int, Text},
        data::{Literal, RowError, ValueError},
        executor::{FetchError, InsertError},
        prelude::{DataType, Payload, Value::*},
    },
    std::borrow::Cow,
};

test_case!(values, async move {
    run!("CREATE TABLE Items (id INTEGER NOT NULL, name TEXT, status TEXT DEFAULT 'ACTIVE' NOT NULL);");

    let test_cases = [
        (
            "VALUES (1), (2), (3)",
            Ok(select!(
                column1;
                I64;
                1;
                2;
                3
            )),
        ),
        (
            "VALUES (1, 'a'), (2, 'b')",
            Ok(select!(
                column1 | column2;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "VALUES (1, 'a'), (2, 'b') ORDER BY column1 DESC",
            Ok(select!(
                column1 | column2;
                I64     | Str;
                2         "b".to_owned();
                1         "a".to_owned()
            )),
        ),
        (
            "VALUES (1), (2) limit 1",
            Ok(select!(
                column1;
                I64;
                1
            )),
        ),
        (
            "VALUES (1), (2) offset 1",
            Ok(select!(
                column1;
                I64;
                2
            )),
        ),
        (
            "VALUES (1, NULL), (2, NULL)",
            Ok(select_with_null!(
                column1 | column2;
                I64(1)    Null;
                I64(2)    Null
            )),
        ),
        (
            "VALUES (1), (2, 'b')",
            Err(RowError::NumberOfValuesDifferent.into()),
        ),
        (
            "VALUES (1, 'a'), (2)",
            Err(RowError::NumberOfValuesDifferent.into()),
        ),
        (
            "VALUES (1, 'a'), (2, 3)",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Text,
                literal: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(3)))),
            }
            .into()),
        ),
        (
            "VALUES (1, 'a'), ('b', 'c')",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Int,
                literal: format!("{:?}", Literal::Text(Cow::Owned("b".to_owned()))),
            }
            .into()),
        ),
        (
            "VALUES (1, NULL), (2, 'a'), (3, 4)",
            Err(ValueError::IncompatibleLiteralForDataType {
                data_type: DataType::Text,
                literal: format!("{:?}", Literal::Number(Cow::Owned(BigDecimal::from(4)))),
            }
            .into()),
        ),
        (
            "CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)",
            Ok(Payload::Create),
        ),
        (
            "SELECT * FROM TableFromValues",
            Ok(select_with_null!(
                column1 | column2         | column3    | column4 | column5;
                I64(1)    Str("a".into())   Bool(true)   Null      Null   ;
                I64(2)    Str("b".into())   Bool(false)  I64(3)    Null
            )),
        ),
        (
            "SHOW COLUMNS FROM TableFromValues",
            Ok(Payload::ShowColumns(vec![
                ("column1".into(), Int),
                ("column2".into(), Text),
                ("column3".into(), Boolean),
                ("column4".into(), Int),
                ("column5".into(), Text)])),
            ),
            (
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived",
            Ok(select!(
                column1 | column2;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "SELECT column1 AS id, column2 AS name FROM (VALUES (1, 'a'), (2, 'b')) AS Derived",
            Ok(select!(
                id      | name;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id)",
            Ok(select!(
                id      | column2;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name)",
            Ok(select!(
                id      | name;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name, dummy)",
            Err(FetchError::TooManyColumnAliases("Derived".into(), 2, 3).into()),
        ),
        (
            "INSERT INTO Items (id) VALUES (1);",
            Ok(Payload::Insert(1))
        ),
        (
            "INSERT INTO Items (id2) VALUES (1);",
            Err(InsertError::WrongColumnName("id2".to_owned()).into()),
        ),
        (
            "INSERT INTO Items (name) VALUES ('glue');",
            Err(InsertError::LackOfRequiredColumn("id".to_owned()).into()),
        ),
        (
            "INSERT INTO Items (id) VALUES (3, 'sql')",
            Err(InsertError::ColumnAndValuesNotMatched.into()),
        ),
        (
            "INSERT INTO Items VALUES (100, 'a', 'b', 1);",
            Err(InsertError::TooManyValues.into()),
        ),
        (
            "INSERT INTO Nothing VALUES (1);",
            Err(InsertError::TableNotFound("Nothing".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
