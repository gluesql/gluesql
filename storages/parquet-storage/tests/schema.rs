use gluesql_core::chrono::NaiveDateTime;
use parquet::data_type::ByteArray;
use {
    gluesql_core::prelude::{
        Glue,
        Value::{self, *},
    },
    gluesql_parquet_storage::ParquetStorage,
    test_suite::{concat_with, row, select, stringify_label},
};

#[tokio::test]
async fn test_alltypes_select() {
    let path = "./tests/samples/parquet_data";
    let parquet_storage = ParquetStorage::new(path).unwrap();
    let mut glue = Glue::new(parquet_storage);

    let bytea = |input: &str| ByteArray::from(input).data().to_vec();

    let ts = |datetime_str| {
        NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%dT%H:%M:%S")
            .expect("Failed to parse date time")
    };

    let cases = vec![
        (
            glue.execute("SELECT * FROM alltypes_dictionary").await,
            Ok(select!(
                id  | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col   | timestamp_col;
                I32 | Bool     | I32         | I32          | I32     | I64        | F32       | F64        | Value::Bytea     | Value::Bytea | Value::Timestamp;
                0    true        0             0              0         0            0.0         0.0          bytea("01/01/09")  bytea("0")     ts("2009-01-01T00:00:00");
                1    false       1             1              1         10           1.1         10.1         bytea("01/01/09")  bytea("1")     ts("2009-01-01T00:01:00")
            )),
        ),
        (
            glue.execute("SELECT * FROM alltypes_plain_snappy").await,
            Ok(select!(
                id  | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col   | timestamp_col;
                I32 | Bool     | I32         | I32          | I32     | I64        | F32       | F64        | Value::Bytea     | Value::Bytea | Value::Timestamp;
                6     true       0             0              0         0            0.0         0.0          bytea("04/01/09")  bytea("0")     ts("2009-04-01T00:00:00");
                7     false      1             1              1         10           1.1         10.1         bytea("04/01/09")  bytea("1")     ts("2009-04-01T00:01:00")
            )),
        ),
        (
            glue.execute("SELECT * FROM alltypes_plain").await,
            Ok(select!(
                id  | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col   | timestamp_col;
                I32 | Bool     | I32         | I32          | I32     | I64        | F32       | F64        | Value::Bytea     | Value::Bytea | Value::Timestamp;
                4     true       0             0              0         0            0.0         0.0          bytea("03/01/09")  bytea("0")     ts("2009-03-01T00:00:00");
                5     false      1             1              1         10           1.1         10.1         bytea("03/01/09")  bytea("1")     ts("2009-03-01T00:01:00");
                6     true       0             0              0         0            0.0         0.0          bytea("04/01/09")  bytea("0")     ts("2009-04-01T00:00:00");
                7     false      1             1              1         10           1.1         10.1         bytea("04/01/09")  bytea("1")     ts("2009-04-01T00:01:00");
                2     true       0             0              0         0            0.0         0.0          bytea("02/01/09")  bytea("0")     ts("2009-02-01T00:00:00");
                3     false      1             1              1         10           1.1         10.1         bytea("02/01/09")  bytea("1")     ts("2009-02-01T00:01:00");
                0     true       0             0              0         0            0.0         0.0          bytea("01/01/09")  bytea("0")     ts("2009-01-01T00:00:00");
                1     false      1             1              1         10           1.1         10.1         bytea("01/01/09")  bytea("1")     ts("2009-01-01T00:01:00")
            )),
        ),
    ];

    //let actual = Payload::SelectMap(actual);

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}

#[tokio::test]
async fn test_schemaless() {
    // let actual = glue
    //     .execute("SELECT * FROM alltypes_dictionary")
    //     .await
    //     .unwrap()
    //     .into_iter()
    //     .next()
    //     .unwrap();

    // let expected = [
    //     vec![
    //         ("id", I32(0)),
    //         ("bool_col", Bool(true)),
    //         ("tinyint_col", I32(0)),
    //         ("smallint_col", I32(0)),
    //         ("int_col", I32(0)),
    //         ("bigint_col", I64(0)),
    //         ("float_col", F32(0.0)),
    //         ("double_col", F64(0.0)),
    //         (
    //             "date_string_col",
    //             Value::Bytea(vec![48, 49, 47, 48, 49, 47, 48, 57]),
    //         ),
    //         ("string_col", Value::Bytea(vec![48])),
    //         ("timestamp_col", I64(0)),
    //     ],
    //     vec![
    //         ("id", I32(1)),
    //         ("bool_col", Bool(false)),
    //         ("tinyint_col", I32(1)),
    //         ("smallint_col", I32(1)),
    //         ("int_col", I32(1)),
    //         ("bigint_col", I64(10)),
    //         ("float_col", F32(1.100000023841858)),
    //         ("double_col", F64(10.1)),
    //         (
    //             "date_string_col",
    //             Value::Bytea(vec![48, 49, 47, 48, 49, 47, 48, 57]),
    //         ),
    //         ("string_col", Value::Bytea(vec![49])),
    //         ("timestamp_col", I64(0)),
    //     ],
    // ]
    // .into_iter()
    // .map(|row| {
    //     row.into_iter()
    //         .map(|(k, v)| (k.to_owned(), v))
    //         .collect::<HashMap<_, _>>()
    // })
    // .collect::<Vec<_>>();
    // let expected = Payload::SelectMap(expected);
    // assert_eq!(actual, expected);
    // let convertu8_to_string =
    //     |input: Vec<u8>| -> Result<String, std::string::FromUtf8Error> { String::from_utf8(input) };
    // let convert_to_bytea = |input: String| -> Vec<u8> { input.into_bytes() };
}

// #[tokio::test]
// async fn test_alltypes_dictionary_fetch_schema() {
//     let path = "./tests/samples/data";
//     let parquet_storage = ParquetStorage::new(path).unwrap();
//     let mut glue = Glue::new(parquet_storage.clone());
//     macro_rules! run {
//         ($sql:expr) => {
//             run($sql, &mut glue, None).await.unwrap()
//         };
//     }
//     run!("SELECT * FROM alltypes_dictionary");
//     let schemas = parquet_storage
//         .fetch_all_schemas()
//         .await
//         .expect("Failed to fetch schemas");

//     let expected_schema = Schema {
//         table_name: "alltypes_dictionary".to_string(),
//         column_defs: Some(vec![
//             ColumnDef {
//                 name: "id".to_string(),
//                 data_type: Int32,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "bool_col".to_string(),
//                 data_type: Boolean,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "tinyint_col".to_string(),
//                 data_type: Int32,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "smallint_col".to_string(),
//                 data_type: Int32,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "int_col".to_string(),
//                 data_type: Int32,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "bigint_col".to_string(),
//                 data_type: Int,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "float_col".to_string(),
//                 data_type: Float32,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "double_col".to_string(),
//                 data_type: Float,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "date_string_col".to_string(),
//                 data_type: DataType::Bytea,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "string_col".to_string(),
//                 data_type: DataType::Bytea,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//             ColumnDef {
//                 name: "timestamp_col".to_string(),
//                 data_type: Int128,
//                 nullable: false,
//                 default: None,
//                 unique: None,
//             },
//         ]),
//         indexes: vec![],
//         engine: None,
//     };

//     // 이 부분은 스키마가 어떻게 저장되고 가져와지는지에 따라 조정해야 할 수 있습니다.
//     assert_eq!(schemas.get(0), Some(&expected_schema));
// }
