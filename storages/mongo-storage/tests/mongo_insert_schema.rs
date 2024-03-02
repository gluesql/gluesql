use {
    gluesql_core::{
        ast::{ColumnDef, DataType},
        data::Schema,
        store::{Store, StoreMut},
    },
    gluesql_mongo_storage::MongoStorage,
};

#[tokio::test]
async fn mongo_insert_schema() {
    let conn_str = "mongodb://localhost:27017";

    let mut storage = MongoStorage::new(conn_str, "mongo_indexes")
        .await
        .expect("MongoStorage::new");
    storage.drop_database().await.expect("database dropped");

    let column_defs = Some(vec![ColumnDef {
        name: "id".to_owned(),
        data_type: DataType::Int,
        nullable: false,
        default: None,
        unique: None,
    }]);

    let mut schema = Schema {
        table_name: "mutable_table".to_owned(),
        column_defs,
        indexes: Vec::new(),
        engine: None,
    };

    storage.insert_schema(&schema).await.unwrap();

    schema.column_defs = schema.column_defs.map(|mut column_defs| {
        column_defs.push(ColumnDef {
            name: "name".to_owned(),
            data_type: DataType::Text,
            nullable: false,
            default: None,
            unique: None,
        });

        column_defs
    });

    storage.insert_schema(&schema).await.unwrap();

    let actual = storage
        .fetch_schema("mutable_table")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(actual, schema);
}
