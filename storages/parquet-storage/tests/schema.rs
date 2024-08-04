use {
    gluesql_core::{
        chrono::NaiveDateTime,
        prelude::{
            Glue, Payload,
            Value::{self, *},
        },
    },
    gluesql_parquet_storage::ParquetStorage,
    parquet::data_type::ByteArray,
    std::fs,
    test_suite::{concat_with, concat_with_null, row, select, select_with_null, stringify_label},
};

struct FileGuard {
    path: String,
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        if let Err(err) = fs::remove_file(&self.path) {
            eprintln!("Failed to remove file: {:?}", err);
        }
    }
}

#[tokio::test]
async fn test_alltypes_select() {
    let path = "./tests/samples/";
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
        (
            glue.execute("SELECT * FROM nested_lists_snappy").await,
            Ok(select!(
                a | b;
                List | I32;
                vec![
                    List(vec![
                        List(vec![Str("a".to_owned()), Str("b".to_owned())]),
                        List(vec![Str("c".to_owned())])
                    ]),
                    List(vec![Null, List(vec![Str("d".to_owned())])])
                ] 1;
                vec![
                    List(vec![
                        List(vec![Str("a".to_owned()), Str("b".to_owned())]),
                        List(vec![Str("c".to_owned()), Str("d".to_owned())])
                    ]),
                    List(vec![Null, List(vec![Str("e".to_owned())])])
                ] 1;
                vec![
                    List(vec![
                        List(vec![Str("a".to_owned()), Str("b".to_owned())]),
                        List(vec![Str("c".to_owned()), Str("d".to_owned())]),
                        List(vec![Str("e".to_owned())])
                    ]),
                    List(vec![Null, List(vec![Str("f".to_owned())])])
                ] 1
            )),
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}

#[tokio::test]
async fn test_data_modify() {
    let path = "./tests/samples/";
    let parquet_storage = ParquetStorage::new(path).unwrap();
    let mut glue = Glue::new(parquet_storage);

    let original_file = "./tests/samples/all_types_with_nulls.parquet"; // adjust the extension if needed
    let copied_file = "./tests/samples/all_types_with_nulls_copy.parquet"; // adjust the extension if needed
    fs::copy(original_file, copied_file).expect("Failed to copy file");

    //invoke delete copy file
    let _file_guard = FileGuard {
        path: copied_file.to_string(),
    };

    let cases = vec![
        (
            glue.execute("SELECT * FROM all_types_with_nulls_copy").await,
            Ok(select_with_null!(
                bool_field | int32_field | int64_field | int96_field | float_field | double_field | binary_field | flba_field;
                Null         Null          Null          Null          Null          Null           Null           Null
            )),
        ),
        (
            glue.execute("INSERT INTO all_types_with_nulls_copy VALUES(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)").await,
            Ok(Payload::Insert(1)),
        ),
        (
            glue.execute("SELECT * FROM all_types_with_nulls_copy").await,
            Ok(select_with_null!(
                bool_field | int32_field | int64_field | int96_field | float_field | double_field | binary_field | flba_field;
                Null         Null          Null          Null          Null          Null           Null           Null;
                Null         Null          Null          Null          Null          Null           Null           Null
            )),
        ),
        (
            glue.execute("DELETE FROM all_types_with_nulls_copy").await,
            Ok(Payload::Delete(2)),
        ),
        (
            glue.execute("SELECT * FROM all_types_with_nulls_copy").await,
            Ok(select!(
                bool_field | int32_field | int64_field | int96_field | float_field | double_field | binary_field | flba_field;
            )),
        ),
        (
            glue.execute("SELECT TABLE_NAME FROM GLUE_TABLES").await,
            Ok(select!(
                TABLE_NAME;
                Str;
                "all_types_with_nulls".to_owned();
                "all_types_with_nulls_copy".to_owned();
                "alltypes_dictionary".to_owned();
                "alltypes_plain".to_owned();
                "alltypes_plain_snappy".to_owned();
                "nested_lists_snappy".to_owned();
                "nested_maps_snappy".to_owned() 
            ))
            ,
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
