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
        unique: None,
        comment: Some("default value is lucky eleven".to_owned()),
    }]);

    let mut schema = Schema {
        table_name: "MutableTable".to_owned(),
        column_defs,
        indexes: Vec::new(),
        engine: None,
    };

    storage.begin(true).await.unwrap();
    storage.insert_schema(&schema).await.unwrap();

    schema.column_defs = schema.column_defs.map(|mut column_defs| {
        column_defs.push(ColumnDef {
            name: "name".to_owned(),
            data_type: DataType::Text,
            nullable: false,
            default: None,
            unique: None,
            comment: Some("this is comment for name column".to_owned()),
        });

        column_defs
    });

    storage.insert_schema(&schema).await.unwrap();
    let actual = storage.fetch_schema("MutableTable").await.unwrap().unwrap();
    let _ = storage.commit().await;

    assert_eq!(
        actual.column_defs, schema.column_defs,
        "Consecutive insert_schema failed"
    );
});
