use {
    crate::*,
    gluesql_core::{
        ast::{AstLiteral, ColumnDef, DataType, Expr},
        data::Schema,
    },
};

test_case!(insert_schema, {
    let storage = &mut get_glue!().storage;
    let column_defs = Some(vec![ColumnDef {
        name: "id".to_owned(),
        data_type: DataType::Int,
        nullable: false,
        default: Some(Expr::Literal(AstLiteral::Number(11.into()))),
        comment: Some("default value is lucky eleven".to_owned()),
    }]);

    let mut schema = Schema {
        table_name: "MutableTable".to_owned(),
        column_defs,
        indexes: Vec::new(),
        engine: None,
        foreign_keys: Vec::new(),
        primary_key: None,
        unique_constraints: Vec::new(),
        comment: Some("this is comment for table".to_owned()),
    };

    storage.begin(true).await.unwrap();
    storage.insert_schema(&schema).await.unwrap();

    schema.column_defs = schema.column_defs.map(|mut column_defs| {
        column_defs.push(ColumnDef {
            name: "name".to_owned(),
            data_type: DataType::Text,
            nullable: false,
            default: None,
            comment: Some("this is comment for name column".to_owned()),
        });

        column_defs
    });

    storage.insert_schema(&schema).await.unwrap();

    let actual = storage.fetch_schema("MutableTable").await.unwrap().unwrap();
    assert_eq!(
        actual.column_defs, schema.column_defs,
        "Consecutive insert_schema failed"
    );
    assert_eq!(actual.comment, schema.comment, "Storing comment failed");

    let schema = Schema {
        table_name: "SchemalessTable".to_owned(),
        column_defs: None,
        indexes: Vec::new(),
        engine: None,
        foreign_keys: Vec::new(),
        primary_key: None,
        unique_constraints: Vec::new(),
        comment: Some("this is comment for schemaless table".to_owned()),
    };
    storage.insert_schema(&schema).await.unwrap();

    let actual = storage
        .fetch_schema("SchemalessTable")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        actual.comment, schema.comment,
        "Storing comment to schemaless table failed"
    );

    let _ = storage.commit().await;
});
