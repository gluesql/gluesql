use {
    chrono::NaiveDate,
    gluesql_core::{
        ast::{ColumnDef, ColumnUniqueOption, DataType},
        data::{Interval, Key, Schema, Value},
        error::Result,
        store::{DataRow, Store, StoreMut},
    },
    gluesql_sqlite_storage::SqliteStorage,
    rust_decimal::Decimal,
    std::net::{IpAddr, Ipv4Addr},
    uuid::Uuid,
};

struct PrimaryKeyCase {
    table: &'static str,
    data_type: DataType,
    value: Value,
    key: Key,
}

fn schema_for(case: &PrimaryKeyCase) -> Schema {
    Schema {
        table_name: case.table.to_owned(),
        column_defs: Some(vec![ColumnDef {
            name: "id".to_owned(),
            data_type: case.data_type.clone(),
            nullable: false,
            default: None,
            unique: Some(ColumnUniqueOption { is_primary: true }),
            comment: None,
        }]),
        indexes: vec![],
        engine: None,
        foreign_keys: vec![],
        comment: None,
    }
}

#[tokio::test]
async fn primary_keys_cover_key_to_sql_variants() -> Result<()> {
    let mut storage = SqliteStorage::memory().await?;

    let uuid = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        .unwrap()
        .as_u128();

    let cases = vec![
        PrimaryKeyCase {
            table: "pk_bool",
            data_type: DataType::Boolean,
            value: Value::Bool(true),
            key: Key::Bool(true),
        },
        PrimaryKeyCase {
            table: "pk_i8",
            data_type: DataType::Int8,
            value: Value::I8(42),
            key: Key::I8(42),
        },
        PrimaryKeyCase {
            table: "pk_i16",
            data_type: DataType::Int16,
            value: Value::I16(1234),
            key: Key::I16(1234),
        },
        PrimaryKeyCase {
            table: "pk_i32",
            data_type: DataType::Int32,
            value: Value::I32(70_000),
            key: Key::I32(70_000),
        },
        PrimaryKeyCase {
            table: "pk_i64",
            data_type: DataType::Int,
            value: Value::I64(-9_876_543_210),
            key: Key::I64(-9_876_543_210),
        },
        PrimaryKeyCase {
            table: "pk_i128",
            data_type: DataType::Int128,
            value: Value::I128(170141183460469231731687303715884105727),
            key: Key::I128(170141183460469231731687303715884105727),
        },
        PrimaryKeyCase {
            table: "pk_u8",
            data_type: DataType::Uint8,
            value: Value::U8(200),
            key: Key::U8(200),
        },
        PrimaryKeyCase {
            table: "pk_u16",
            data_type: DataType::Uint16,
            value: Value::U16(40_000),
            key: Key::U16(40_000),
        },
        PrimaryKeyCase {
            table: "pk_u32",
            data_type: DataType::Uint32,
            value: Value::U32(3_000_000_000),
            key: Key::U32(3_000_000_000),
        },
        PrimaryKeyCase {
            table: "pk_u64",
            data_type: DataType::Uint64,
            value: Value::U64(18_446_744_073_709_551_615),
            key: Key::U64(18_446_744_073_709_551_615),
        },
        PrimaryKeyCase {
            table: "pk_u128",
            data_type: DataType::Uint128,
            value: Value::U128(340282366920938463463374607431768211455),
            key: Key::U128(340282366920938463463374607431768211455),
        },
        PrimaryKeyCase {
            table: "pk_decimal",
            data_type: DataType::Decimal,
            value: Value::Decimal("1234567890.1234567890".parse::<Decimal>().unwrap()),
            key: Key::Decimal("1234567890.1234567890".parse::<Decimal>().unwrap()),
        },
        PrimaryKeyCase {
            table: "pk_text",
            data_type: DataType::Text,
            value: Value::Str("primary-key".to_owned()),
            key: Key::Str("primary-key".to_owned()),
        },
        PrimaryKeyCase {
            table: "pk_bytea",
            data_type: DataType::Bytea,
            value: Value::Bytea(vec![1, 2, 3, 4]),
            key: Key::Bytea(vec![1, 2, 3, 4]),
        },
        PrimaryKeyCase {
            table: "pk_date",
            data_type: DataType::Date,
            value: Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            key: Key::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        },
        PrimaryKeyCase {
            table: "pk_timestamp",
            data_type: DataType::Timestamp,
            value: Value::Timestamp(
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 1)
                    .unwrap(),
            ),
            key: Key::Timestamp(
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 1)
                    .unwrap(),
            ),
        },
        PrimaryKeyCase {
            table: "pk_interval",
            data_type: DataType::Interval,
            value: Value::Interval(Interval::days(1)),
            key: Key::Interval(Interval::days(1)),
        },
        PrimaryKeyCase {
            table: "pk_uuid",
            data_type: DataType::Uuid,
            value: Value::Uuid(uuid),
            key: Key::Uuid(uuid),
        },
        PrimaryKeyCase {
            table: "pk_inet",
            data_type: DataType::Inet,
            value: Value::Inet(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1))),
            key: Key::Inet(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1))),
        },
    ];

    for case in cases {
        let schema = schema_for(&case);

        storage.insert_schema(&schema).await?;

        storage
            .insert_data(
                case.table,
                vec![(case.key.clone(), DataRow::Vec(vec![case.value.clone()]))],
            )
            .await?;

        let fetched = storage
            .fetch_data(case.table, &case.key)
            .await?
            .expect("primary key row missing");

        assert_eq!(fetched, DataRow::Vec(vec![case.value.clone()]));

        storage.delete_data(case.table, vec![case.key]).await?;
    }

    Ok(())
}
