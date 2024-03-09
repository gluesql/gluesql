use {
    crate::*,
    gluesql_core::{
        ast::{ColumnDef, DataType},
        data::Schema,
    },
};

test_case!(insert_schema, {
    let storage = &mut get_glue!().storage;
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
});
