use crate::*;

test_case!(auto_increment, async move {
    use Value::*;

    let test_cases = vec![
        (
            "CREATE TABLE Test (id INTEGER AUTO_INCREMENT NOT NULL, name TEXT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO Test (name) VALUES ('test1')"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT * FROM Test"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned())),
        ),
        (
            r#"INSERT INTO Test (name) VALUES ('test2'), ('test3')"#,
            Ok(Payload::Insert(2)),
        ),
        (
            r#"SELECT * FROM Test"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            2    "test2".to_owned();
            3    "test3".to_owned())),
        ),
        (
            r#"INSERT INTO Test (name, id) VALUES ('test4', NULL)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT * FROM Test"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            2    "test2".to_owned();
            3    "test3".to_owned();
            4    "test4".to_owned())),
        ),
        (
            r#"INSERT INTO Test (name, id) VALUES ('test5', 6)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT * FROM Test"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            2    "test2".to_owned();
            3    "test3".to_owned();
            4    "test4".to_owned();
            6    "test5".to_owned())),
        ),
        (
            r#"INSERT INTO Test (name) VALUES ('test6')"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT * FROM Test"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            2    "test2".to_owned();
            3    "test3".to_owned();
            4    "test4".to_owned();
            6    "test5".to_owned();
            5    "test6".to_owned())),
        ),
        (
            r#"INSERT INTO Test (name) VALUES ('test7')"#,
            Ok(Payload::Insert(1)),
        ),
        (
            r#"SELECT * FROM Test"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            2    "test2".to_owned();
            3    "test3".to_owned();
            4    "test4".to_owned();
            6    "test5".to_owned();
            5    "test6".to_owned();
            6    "test7".to_owned())),
        ),
        (
            "CREATE TABLE TestUnique (id INTEGER AUTO_INCREMENT NOT NULL UNIQUE, name TEXT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO TestUnique (name, id) VALUES ('test1', NULL), ('test2', 3)"#,
            Ok(Payload::Insert(2)),
        ),
        (
            r#"SELECT * FROM TestUnique"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            3    "test2".to_owned())),
        ),
        (
            r#"INSERT INTO TestUnique (name) VALUES ('test3'), ('test4')"#,
            Err(ValidateError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(3)),
                "id".to_owned(),
            )
            .into()),
        ),
        (
            r#"SELECT * FROM TestUnique"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            3    "test2".to_owned())),
        ),
        (
            "CREATE TABLE TestUniqueSecond (id INTEGER AUTO_INCREMENT NOT NULL UNIQUE, name TEXT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO TestUniqueSecond (name, id) VALUES ('test1', NULL), ('test2', 3), ('test3', NULL), ('test4', NULL)"#,
            Err(ValidateError::DuplicateEntryOnUniqueField(
                format!("{:?}", Value::I64(3)),
                "id".to_owned(),
            )
            .into()),
        ),
        /*( Broken at the moment, see gluesql#190
            "CREATE TABLE TestInsertSelect (id INTEGER AUTO_INCREMENT NOT NULL, name TEXT)",
            Ok(Payload::Create),
        ),
        (
            r#"INSERT INTO TestInsertSelect (name) SELECT name FROM Test"#,
            Ok(Payload::Insert(7)),
        ),
        (
            r#"SELECT * FROM TestInsertSelect"#,
            Ok(select!(
            id  | name
            I64 | Str;
            1    "test1".to_owned();
            2    "test2".to_owned();
            3    "test3".to_owned();
            4    "test4".to_owned();
            5    "test5".to_owned();
            6    "test6".to_owned();
            7    "test7".to_owned())),
        ),*/
        (
            "CREATE TABLE TestText (id TEXT AUTO_INCREMENT NOT NULL UNIQUE, name TEXT)",
            Err(AlterError::UnsupportedDataTypeForAutoIncrementColumn(
                "id".to_owned(),
                "TEXT".to_owned(),
            )
            .into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
