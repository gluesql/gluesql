use {
    gluesql_core::prelude::Glue,
    gluesql_parquet_storage::{error::ParquetStorageError, ParquetStorage},
};

#[tokio::test]
async fn test_from_parquet_storage_error_to_error() {
    let path_str = "./tests/samples/";
    let parquet_storage = ParquetStorage::new(path_str).unwrap();
    let mut glue = Glue::new(parquet_storage);

    let cases = vec![(
        glue.execute("SELECT * FROM nested_maps_snappy").await,
        Err(ParquetStorageError::UnexpectedKeyTypeForMap("Int(1)".to_owned()).into()),
    )];

    for (actual, expected) in cases {
        assert_eq!(actual.map(|mut payloads| payloads.remove(0)), expected);
    }
}
