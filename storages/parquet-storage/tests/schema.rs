use parquet::data_type::ByteArray;

use {
    gluesql_core::{
        chrono::NaiveDateTime,
        error::Error,
        prelude::{
            Glue,
            Value::{self, *},
        },
    },
    gluesql_parquet_storage::ParquetStorage,
    test_suite::{concat_with, row, select, stringify_label},
};

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
        (
            glue.execute("SELECT * FROM nested_maps_snappy").await,
            Err(Error::StorageMsg(
                "Unexpected key type for map: received Int(1), expected String".to_string(),
            )),
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
