use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption, ToSql},
    chrono::format,
    prelude::DataType,
    store::Store,
};
use mongodb::{
    bson::{self, bson, doc, Bson, Document},
    options::{CreateCollectionOptions, IndexOptions, ReplaceOptions, UpdateOptions},
    Collection,
};

use crate::{
    store::{B16, B32, B8, TIME},
    utils::get_primary_key,
    value::{into_object_id, BsonType, IntoBson},
};

use {
    crate::{error::ResultExt, MongoStorage},
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, StoreMut},
    },
    serde_json::{to_string_pretty, Map, Value as JsonValue},
    std::{
        fs::File,
        io::Write,
        {cmp::Ordering, iter::Peekable, vec::IntoIter},
    },
};

struct IndexInfo {
    name: String, // TODO: convert to enum with primary and unique
    key: String,
}

#[async_trait(?Send)]
impl StoreMut for MongoStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let (names, column_types, indexes) = schema
            .column_defs
            .as_ref()
            .map(|column_defs| {
                column_defs.iter().fold(
                    (vec![], doc! {}, vec![]),
                    |(mut names, mut column_types, mut indexes), column_def| {
                        let column_name = column_def.name.clone();
                        names.push(column_name.clone());
                        let data_type = BsonType::from(&column_def.data_type).into();
                        let maximum = match column_def.data_type {
                            DataType::Int8 => Some(B8),
                            DataType::Int16 => Some(B16),
                            DataType::Int32 => Some(B32),
                            DataType::Float32 => Some(B32),
                            DataType::Time => Some(TIME),
                            _ => None,
                        };

                        let mut bson_type = match column_def.clone().nullable {
                            true => vec![data_type, "null"],
                            false => vec![data_type],
                        };

                        match &column_def.unique {
                            Some(ColumnUniqueOption { is_primary }) => match *is_primary {
                                true => {
                                    indexes.push(IndexInfo {
                                        name: format!("{column_name}_PK"),
                                        key: column_name.clone(),
                                    });
                                }
                                false => {
                                    bson_type = vec![data_type, "null"];
                                    indexes.push(IndexInfo {
                                        name: format!("{column_name}_UNIQUE"),
                                        key: column_name.clone(),
                                    });
                                }
                            },
                            None => {}
                        }

                        let mut property = doc! {
                            "bsonType": bson_type,
                        };

                        if let Some(maximum) = maximum {
                            property.extend(doc! {
                                "maximum": maximum,
                            });
                        }

                        if let Some(default) = &column_def.default {
                            property.extend(doc! {
                                "description": default.to_sql()
                            });
                        }

                        let type_str = column_def.data_type.to_string();
                        property.extend(doc! {
                            "title": type_str
                        });

                        let column_type = doc! {
                            column_name: property,
                        };

                        column_types.extend(column_type);

                        (names, column_types, indexes)
                    },
                )
                // [(name1, type1), (name2, type2), ..]
                // [name1, name2, ..], [type1, type2, ..]
            })
            .unwrap_or_default();

        // let required = bson!["_id", names];
        let mut properties = doc! {
            "_id": { "bsonType": "objectId" }
        };
        properties.extend(column_types);
        let mut required = vec!["_id".to_string()];
        required.extend(names.clone());

        let additional_properties = match names.len() {
            0 => true,
            _ => false,
        };

        let option = CreateCollectionOptions::builder()
            .validator(Some(doc! {
                "$jsonSchema": {
                    "type": "object",
                    "required": required,
                    "properties": properties,
                    "additionalProperties": additional_properties
                  }
            }))
            .build();

        println!("option: {}", to_string_pretty(&option).unwrap());

        self.db
            .create_collection(&schema.table_name, option)
            .await
            .map_storage_err()?;

        if indexes.is_empty() {
            return Ok(());
        }

        let index_models = indexes
            .into_iter()
            .map(|IndexInfo { name, key }| {
                let index_options = IndexOptions::builder().unique(true);
                let index_options = match name.split_once("_") {
                    Some((_, "UNIQUE")) => index_options
                        .partial_filter_expression(
                            doc! { "partialFilterExpression": { name.clone(): { "$en": null } } },
                        )
                        // .sparse(true)
                        .name(name)
                        .build(),
                    _ => index_options.name(name).build(),
                };

                mongodb::IndexModel::builder()
                    .keys(doc! {key: 1})
                    .options(index_options)
                    .build()
            })
            .collect::<Vec<_>>();

        self.db
            .collection::<Document>(&schema.table_name)
            .create_indexes(index_models, None)
            .await
            .map(|_| ())
            .map_storage_err()
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.db
            .collection::<Document>(table_name)
            .drop(None)
            .await
            .map_storage_err()
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        println!("append_data");
        println!("rows: {:?}", rows);

        let column_defs = self
            .fetch_schema(table_name)
            .await
            .unwrap()
            .unwrap()
            .column_defs;

        let data = rows
            .into_iter()
            .map(|row| match row {
                DataRow::Vec(values) => column_defs
                    .clone()
                    .unwrap()
                    .into_iter()
                    .zip(values.into_iter())
                    .fold(Document::new(), |mut acc, (column_def, value)| {
                        acc.extend(doc! {column_def.name: value.into_bson().unwrap()});

                        acc
                    }),
                DataRow::Map(hash_map) => {
                    hash_map
                        .into_iter()
                        .fold(Document::new(), |mut acc, (key, value)| {
                            acc.extend(doc! {key: value.into_bson().unwrap()});

                            acc
                        })
                }
            })
            .collect::<Vec<_>>();

        if data.is_empty() {
            return Ok(());
        }

        self.db
            .collection::<Document>(table_name)
            .insert_many(data, None)
            .await
            .map(|_| ())
            .map_storage_err()
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        println!("insert_data");

        let column_defs = self
            .fetch_schema(table_name)
            .await
            .unwrap()
            .unwrap()
            .column_defs;

        let primary_key = column_defs.clone().map(get_primary_key).flatten();
        // &get_primary_key(column_defs.clone().unwrap());

        for (key, row) in rows {
            let doc = match row {
                DataRow::Vec(values) => {
                    column_defs
                        .clone()
                        .unwrap()
                        .into_iter()
                        .zip(values.into_iter())
                        .fold(
                            // usualy key is _id, but if there is PK, key is PK column
                            match primary_key {
                                Some(_) => doc! {},
                                None => doc! {"_id": into_object_id(key.clone())},
                            },
                            |mut acc, (column_def, value)| {
                                acc.extend(doc! {column_def.name: value.into_bson().unwrap()});

                                acc
                            },
                        )
                }
                DataRow::Map(hash_map) => hash_map.into_iter().fold(
                    doc! {"_id": into_object_id(key.clone())},
                    |mut acc, (key, value)| {
                        acc.extend(doc! {key: value.into_bson().unwrap()});

                        acc
                    },
                ),
            };
            println!("doc: {:#?}", doc);

            // 1. pk insert
            // 2. update
            // 3. pk update?
            let query = match &primary_key {
                Some(column_def) => doc! {column_def.name.clone(): key.into_bson().unwrap()},
                _ => doc! {"_id": into_object_id(key.clone())},
            };

            // let doc = doc! {"$set": doc};
            // let options = UpdateOptions::builder().upsert(Some(true)).build();
            let options = ReplaceOptions::builder().upsert(Some(true)).build();

            println!("query: {:#?}", query);
            println!("doc: {:#?}", doc);
            println!("options: {:#?}", options);

            self.db
                .collection::<Document>(table_name)
                .replace_one(query, doc, options)
                .await
                .unwrap();
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        println!("keys: {keys:?}");

        self.db
            .collection::<Bson>(table_name)
            .delete_many(
                doc! { "_id": {
                    "$in": keys.into_iter().map(|key| key.into_bson()).collect::<Result<Vec<_>, _>>()?
                }},
                None,
            )
            .await
            .map(|_| ())
            .map_storage_err()
    }
}

struct SortMerge<T: Iterator<Item = Result<(Key, DataRow)>>> {
    left_rows: Peekable<T>,
    right_rows: Peekable<IntoIter<(Key, DataRow)>>,
}

impl<T> SortMerge<T>
where
    T: Iterator<Item = Result<(Key, DataRow)>>,
{
    fn new(left_rows: T, right_rows: IntoIter<(Key, DataRow)>) -> Self {
        let left_rows = left_rows.peekable();
        let right_rows = right_rows.peekable();

        Self {
            left_rows,
            right_rows,
        }
    }
}
impl<T> Iterator for SortMerge<T>
where
    T: Iterator<Item = Result<(Key, DataRow)>>,
{
    type Item = Result<DataRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let left = self.left_rows.peek();
        let right = self.right_rows.peek();

        match (left, right) {
            (Some(Ok((left_key, _))), Some((right_key, _))) => match left_key.cmp(right_key) {
                Ordering::Less => self.left_rows.next(),
                Ordering::Greater => self.right_rows.next().map(Ok),
                Ordering::Equal => {
                    self.left_rows.next();
                    self.right_rows.next().map(Ok)
                }
            }
            .map(|item| Ok(item?.1)),
            (Some(_), _) => self.left_rows.next().map(|item| Ok(item?.1)),
            (None, Some(_)) => self.right_rows.next().map(|item| Ok(item.1)),
            (None, None) => None,
        }
    }
}

impl MongoStorage {
    fn rewrite(&mut self, schema: Schema, rows: Vec<DataRow>) -> Result<()> {
        todo!();
    }

    fn write(
        &mut self,
        schema: Schema,
        rows: Vec<DataRow>,
        mut file: File,
        is_json: bool,
    ) -> Result<()> {
        let column_defs = schema.column_defs.unwrap_or_default();
        let labels = column_defs
            .iter()
            .map(|column_def| column_def.name.as_str())
            .collect::<Vec<_>>();
        let rows = rows
            .into_iter()
            .map(|row| match row {
                DataRow::Vec(values) => labels
                    .iter()
                    .zip(values.into_iter())
                    .map(|(key, value)| Ok((key.to_string(), value.try_into()?)))
                    .collect::<Result<Map<String, JsonValue>>>(),
                DataRow::Map(hash_map) => hash_map
                    .into_iter()
                    .map(|(key, value)| Ok((key, value.try_into()?)))
                    .collect(),
            })
            .map(|result| result.map(JsonValue::Object));

        if is_json {
            let rows = rows.collect::<Result<Vec<_>>>().and_then(|rows| {
                let rows = JsonValue::Array(rows);

                to_string_pretty(&rows).map_storage_err()
            })?;

            file.write_all(rows.as_bytes()).map_storage_err()?;
        } else {
            for row in rows {
                let row = row?;

                writeln!(file, "{row}").map_storage_err()?;
            }
        }

        Ok(())
    }
}
