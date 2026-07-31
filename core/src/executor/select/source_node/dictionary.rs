use {
    super::{super::SourceColumns, PreparedSource, SourceRows},
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, Dictionary, ToSql, ToSqlUnquoted},
        data::{Row, Value},
        executor::context::RowContext,
        plan::DictionarySourcePlan,
        result::Result,
        store::GStore,
    },
    std::{collections::BTreeMap, iter, rc::Rc},
};

pub(super) fn execute<'a, T: GStore>(
    storage: &'a T,
    dictionary: &'a DictionarySourcePlan,
) -> PreparedSource<'a> {
    let names = match dictionary.dictionary {
        Dictionary::GlueObjects => vec![
            "OBJECT_NAME".to_owned(),
            "OBJECT_TYPE".to_owned(),
            "CREATED".to_owned(),
        ],
        Dictionary::GlueTables => vec!["TABLE_NAME".to_owned(), "COMMENT".to_owned()],
        Dictionary::GlueTableColumns => vec![
            "TABLE_NAME".to_owned(),
            "COLUMN_NAME".to_owned(),
            "COLUMN_ID".to_owned(),
            "NULLABLE".to_owned(),
            "KEY".to_owned(),
            "DEFAULT".to_owned(),
            "COMMENT".to_owned(),
        ],
        Dictionary::GlueIndexes => vec![
            "TABLE_NAME".to_owned(),
            "INDEX_NAME".to_owned(),
            "ORDER".to_owned(),
            "EXPRESSION".to_owned(),
            "UNIQUENESS".to_owned(),
        ],
    };

    let output = SourceColumns {
        alias: &dictionary.alias.name,
        names: Rc::from(names),
    };
    let source = SourceColumns {
        alias: output.alias,
        names: Rc::clone(&output.names),
    };
    let rows = Box::new(move |_: Option<Rc<RowContext<'a>>>| {
        rows(
            storage,
            dictionary,
            SourceColumns {
                alias: source.alias,
                names: Rc::clone(&source.names),
            },
        )
    });

    PreparedSource { output, rows }
}

fn rows<'a, T: GStore>(
    storage: &'a T,
    dictionary: &'a DictionarySourcePlan,
    source: SourceColumns<'a>,
) -> Result<SourceRows<'a>> {
    let columns = Rc::clone(&source.names);
    let rows = match &dictionary.dictionary {
        Dictionary::GlueObjects => {
            let schemas = storage.fetch_all_schemas()?;
            let table_metas = storage
                .scan_table_meta()?
                .collect::<Result<BTreeMap<_, _>>>()?;
            let rows = schemas.into_iter().flat_map({
                let columns = Rc::clone(&columns);

                move |schema| {
                    let meta = table_metas
                        .iter()
                        .find_map(|(table_name, hash_map)| {
                            (table_name == &schema.table_name).then(|| hash_map.clone())
                        })
                        .unwrap_or_default();
                    let table_rows = BTreeMap::from([
                        ("OBJECT_NAME".to_owned(), Value::Str(schema.table_name)),
                        ("OBJECT_TYPE".to_owned(), Value::Str("TABLE".to_owned())),
                    ])
                    .into_iter()
                    .chain(meta)
                    .collect::<BTreeMap<_, _>>();
                    let index_rows = schema.indexes.into_iter().map(|index| {
                        BTreeMap::from([
                            ("OBJECT_NAME".to_owned(), Value::Str(index.name)),
                            ("OBJECT_TYPE".to_owned(), Value::Str("INDEX".to_owned())),
                        ])
                    });
                    let columns = Rc::clone(&columns);

                    iter::once(table_rows)
                        .chain(index_rows)
                        .map(move |mut hash_map| Row {
                            values: columns
                                .iter()
                                .map(|column| hash_map.remove(column).unwrap_or(Value::Null))
                                .collect(),
                            columns: Rc::clone(&columns),
                        })
                }
            });

            Box::new(rows.map(Ok)) as Box<dyn Iterator<Item = Result<Row>> + 'a>
        }
        Dictionary::GlueTables => {
            let schemas = storage.fetch_all_schemas()?;
            let rows = schemas.into_iter().map({
                let columns = Rc::clone(&columns);

                move |schema| {
                    Ok(Row {
                        columns: Rc::clone(&columns),
                        values: vec![
                            Value::Str(schema.table_name),
                            schema.comment.map_or(Value::Null, Value::Str),
                        ],
                    })
                }
            });

            Box::new(rows)
        }
        Dictionary::GlueTableColumns => {
            let schemas = storage.fetch_all_schemas()?;
            let rows = schemas.into_iter().flat_map({
                let columns = Rc::clone(&columns);

                move |schema| {
                    let columns = Rc::clone(&columns);
                    let table_name = schema.table_name;

                    schema
                        .column_defs
                        .unwrap_or_default()
                        .into_iter()
                        .enumerate()
                        .map(move |(index, column_def)| {
                            let values = vec![
                                Value::Str(table_name.clone()),
                                Value::Str(column_def.name),
                                Value::I64(index as i64 + 1),
                                Value::Bool(column_def.nullable),
                                column_def
                                    .unique
                                    .map_or(Value::Null, |unique| Value::Str(unique.to_sql())),
                                column_def
                                    .default
                                    .map_or(Value::Null, |expr| Value::Str(expr.to_sql())),
                                column_def.comment.map_or(Value::Null, Value::Str),
                            ];

                            Row {
                                columns: Rc::clone(&columns),
                                values,
                            }
                        })
                }
            });

            Box::new(rows.map(Ok))
        }
        Dictionary::GlueIndexes => {
            let schemas = storage.fetch_all_schemas()?;
            let rows = schemas.into_iter().flat_map({
                let columns = Rc::clone(&columns);

                move |schema| {
                    let column_defs = schema.column_defs.unwrap_or_default();
                    let primary_column = column_defs.iter().find_map(|column_def| {
                        let ColumnDef { name, unique, .. } = column_def;

                        (unique == &Some(ColumnUniqueOption { is_primary: true })).then_some(name)
                    });
                    let clustered = match primary_column {
                        Some(column_name) => {
                            let values = vec![
                                Value::Str(schema.table_name.clone()),
                                Value::Str("PRIMARY".to_owned()),
                                Value::Str("BOTH".to_owned()),
                                Value::Str(column_name.to_owned()),
                                Value::Bool(true),
                            ];

                            vec![Row {
                                columns: Rc::clone(&columns),
                                values,
                            }]
                        }
                        None => Vec::new(),
                    };
                    let columns = Rc::clone(&columns);
                    let non_clustered = schema.indexes.into_iter().map(move |index| {
                        let values = vec![
                            Value::Str(schema.table_name.clone()),
                            Value::Str(index.name),
                            Value::Str(index.order.to_string()),
                            Value::Str(index.expr.to_sql_unquoted()),
                            Value::Bool(false),
                        ];

                        Row {
                            columns: Rc::clone(&columns),
                            values,
                        }
                    });

                    clustered.into_iter().chain(non_clustered)
                }
            });

            Box::new(rows.map(Ok))
        }
    };

    Ok(SourceRows { source, rows })
}
