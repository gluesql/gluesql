use {
    gluesql_core::{
        chrono::{NaiveDate, NaiveTime},
        data::{Key, Schema, Value},
        error::Result,
        store::{ScanColumns, Store, StoreMut},
    },
    gluesql_parquet_storage::{ParquetStorage, error::ParquetStorageError},
    parquet::{
        data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type},
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        format::KeyValue,
        schema::parser::parse_message_type,
    },
    std::{
        collections::BTreeMap,
        fs::{self, File},
        sync::Arc,
    },
    uuid::Uuid,
};

#[test]
fn missing_table() {
    let path = std::env::temp_dir().join(format!("parquet-projection-{}", Uuid::new_v4()));
    let storage = ParquetStorage::new(&path).unwrap();

    let actual = storage.scan_data("Missing", ScanColumns::All).map(|_| ());
    let expected = Err(ParquetStorageError::TableDoesNotExist("Missing".to_owned()).into());
    assert_eq!(actual, expected);

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn empty_projection_across_row_groups() {
    let path = std::env::temp_dir().join(format!("parquet-projection-{}", Uuid::new_v4()));
    let storage = ParquetStorage::new(&path).unwrap();
    let schema = Arc::new(
        parse_message_type("message schema { REQUIRED INT64 id; REQUIRED INT64 ignored; }")
            .unwrap(),
    );
    let cases = [
        (
            "WithKey",
            vec![KeyValue {
                key: "unique_optionid".to_owned(),
                value: Some("primary_key".to_owned()),
            }],
            vec![(Key::I64(7), vec![]), (Key::I64(11), vec![])],
        ),
        (
            "WithoutKey",
            vec![],
            vec![(Key::U64(0), vec![]), (Key::U64(1), vec![])],
        ),
    ];

    for (table, metadata, expected) in cases {
        let properties = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(Some(metadata))
                .build(),
        );
        let file = File::create(path.join(format!("{table}.parquet"))).unwrap();
        let mut writer = SerializedFileWriter::new(file, Arc::clone(&schema), properties).unwrap();

        for values in [[7, 70], [11, 110]] {
            let mut group = writer.next_row_group().unwrap();
            for value in values {
                let mut column = group.next_column().unwrap().unwrap();
                column
                    .typed::<Int64Type>()
                    .write_batch(&[value], None, None)
                    .unwrap();
                column.close().unwrap();
            }
            group.close().unwrap();
        }
        writer.close().unwrap();

        let actual = storage
            .scan_data(table, ScanColumns::Selected(&[]))
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            actual, expected,
            "{table}: row groups retain their keys and row counts"
        );
    }

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn projected_fields_use_original_schema_positions() {
    let path = std::env::temp_dir().join(format!("parquet-projection-{}", Uuid::new_v4()));
    let mut storage = ParquetStorage::new(&path).unwrap();
    let schema = Schema::from_ddl(
        "CREATE TABLE Item (id INTEGER PRIMARY KEY, ignored TEXT, at TIME, token UUID, stamped TIMESTAMP)",
    ).unwrap();
    let at = NaiveTime::from_hms_micro_opt(12, 34, 56, 789).unwrap();
    let stamped = NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(3, 4, 5)
        .unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .insert_data(
            "Item",
            vec![(
                Key::I64(7),
                vec![
                    Value::I64(7),
                    Value::Str("unused".to_owned()),
                    Value::Time(at),
                    Value::Uuid(42),
                    Value::Timestamp(stamped),
                ],
            )],
        )
        .unwrap();

    let rows = {
        let columns = [
            ("at".to_owned(), 2),
            ("token".to_owned(), 3),
            ("stamped".to_owned(), 4),
        ];
        storage
            .scan_data("Item", ScanColumns::Selected(&columns))
            .unwrap()
    };
    let actual = rows.collect::<Result<Vec<_>>>().unwrap();
    let expected = vec![(
        Key::I64(7),
        vec![Value::Time(at), Value::Uuid(42), Value::Timestamp(stamped)],
    )];
    assert_eq!(actual, expected);

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn projected_nested_maps() {
    let path = std::env::temp_dir().join(format!("parquet-projection-{}", Uuid::new_v4()));
    let storage = ParquetStorage::new(&path).unwrap();
    let schema = Arc::new(
        parse_message_type(
            "message schema {
                REQUIRED INT64 ignored;
                REQUIRED GROUP profile {
                    REQUIRED GROUP scores (MAP) {
                        REPEATED GROUP key_value {
                            REQUIRED BINARY key (UTF8);
                            REQUIRED INT32 value;
                        }
                    }
                }
            }",
        )
        .unwrap(),
    );
    let properties = Arc::new(WriterProperties::builder().build());
    let file = File::create(path.join("Student.parquet")).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, properties).unwrap();
    let mut group = writer.next_row_group().unwrap();

    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&[7], None, None)
        .unwrap();
    column.close().unwrap();

    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<ByteArrayType>()
        .write_batch(
            &[ByteArray::from("math"), ByteArray::from("art")],
            Some(&[1, 1]),
            Some(&[0, 1]),
        )
        .unwrap();
    column.close().unwrap();

    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int32Type>()
        .write_batch(&[90, 70], Some(&[1, 1]), Some(&[0, 1]))
        .unwrap();
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();

    let columns = [("profile".to_owned(), 1)];
    let actual = storage
        .scan_data("Student", ScanColumns::Selected(&columns))
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let expected = vec![(
        Key::U64(0),
        vec![Value::Map(BTreeMap::from([(
            "scores".to_owned(),
            Value::Map(BTreeMap::from([
                ("art".to_owned(), Value::I32(70)),
                ("math".to_owned(), Value::I32(90)),
            ])),
        )]))],
    )];
    assert_eq!(actual, expected);

    fs::remove_dir_all(path).unwrap();
}
