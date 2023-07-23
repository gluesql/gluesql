use gluesql_core::store::Store;
use mongodb::{
    bson::{self, bson, doc, Bson, Document},
    options::{CreateCollectionOptions, UpdateOptions},
    Collection,
};

use crate::value::{
    get_element_type_string, into_bson_key, into_object_id, IntoBson, IntoBsonType,
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

#[async_trait(?Send)]
impl StoreMut for MongoStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let (names, column_types) = schema
            .column_defs
            .as_ref()
            .map(|column_defs| {
                column_defs.iter().fold(
                    (vec![], doc! {}),
                    |(mut names, mut column_types), column_def| {
                        let column_name = column_def.name.clone();
                        names.push(column_name.clone());
                        let nullable = match column_def.clone().nullable {
                            true => Some("null"),
                            false => None,
                        };
                        let column_type = doc! {
                            column_name: {
                                "bsonType": [get_element_type_string(column_def.data_type.into_bson_type()), nullable],
                            },
                        };
                        column_types.extend(column_type);

                        (names, column_types)
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
        required.extend(names);

        let option = CreateCollectionOptions::builder()
            .validator(Some(doc! {
                "$jsonSchema": {
                    "type": "object",
                    "required": required,
                    "properties": properties,
                    "additionalProperties": false
                  }
            }))
            .build();

        self.db
            .create_collection(&schema.table_name, option)
            .await
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

        let column_defs = self
            .fetch_schema(table_name)
            .await
            .unwrap()
            .unwrap()
            .column_defs
            .unwrap();

        let data = rows
            .into_iter()
            .map(|row| match row {
                DataRow::Vec(values) => column_defs
                    .clone()
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

        self.db
            .collection::<Document>(table_name)
            .insert_many(data, None)
            .await
            .map(|_| ())
            .map_storage_err()
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        println!("insert_data");

        let column_defs = self
            .fetch_schema(table_name)
            .await
            .unwrap()
            .unwrap()
            .column_defs
            .unwrap();

        for (key, row) in rows {
            let doc = match row {
                DataRow::Vec(values) => column_defs
                    .clone()
                    .into_iter()
                    .zip(values.into_iter())
                    .fold(
                        doc! {"_id": into_object_id(key.clone())},
                        |mut acc, (column_def, value)| {
                            acc.extend(doc! {column_def.name: value.into_bson().unwrap()});

                            acc
                        },
                    ),
                DataRow::Map(hash_map) => hash_map.into_iter().fold(
                    doc! {"_id": into_object_id(key.clone())},
                    |mut acc, (key, value)| {
                        acc.extend(doc! {key: value.into_bson().unwrap()});

                        acc
                    },
                ),
            };

            // TODO: for update, delete the _id before. what about pk insert? should we delete the _id?
            self.db
                .collection::<Document>(table_name)
                .delete_one(doc! {"_id": into_object_id(key)}, None)
                .await
                .map_storage_err()?;

            self.db
                .collection::<Document>(table_name)
                .insert_one(doc, None)
                .await
                .map_storage_err()?;
        }

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
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
