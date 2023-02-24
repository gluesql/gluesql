use {
    crate::{
        error::{JsonlStorageError, OptionExt, ResultExt},
        JsonlStorage,
    },
    async_trait::async_trait,
    gluesql_core::{data::Schema, prelude::Key, result::Result, store::DataRow, store::StoreMut},
    serde_json::{Map, Value as JsonValue},
    std::{
        fs::{remove_file, File, OpenOptions},
        io::Write,
        {cmp::Ordering, iter::Peekable, vec::IntoIter},
    },
};

#[async_trait(?Send)]
impl StoreMut for JsonlStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_path = self.data_path(schema.table_name.as_str());
        File::create(data_path).map_storage_err()?;

        if schema.column_defs.is_some() {
            let schema_path = self.schema_path(schema.table_name.as_str());
            let ddl = schema.to_ddl();
            let mut file = File::create(schema_path).map_storage_err()?;
            write!(file, "{ddl}").map_storage_err()?;
        }

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let data_path = self.data_path(table_name);
        if data_path.exists() {
            remove_file(data_path).map_storage_err()?;
        }

        let schema_path = self.schema_path(table_name);
        if schema_path.exists() {
            remove_file(schema_path).map_storage_err()?;
        }

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let schema = self
            .fetch_schema(table_name)?
            .map_storage_err(JsonlStorageError::TableDoesNotExist)?;
        let table_path = self.data_path(table_name);

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(table_path)
            .map_storage_err()?;

        let column_defs = schema.column_defs.unwrap_or_default();
        let labels = column_defs
            .iter()
            .map(|column_def| column_def.name.as_str())
            .collect::<Vec<_>>();

        for row in rows {
            let json_string = match row {
                DataRow::Vec(values) => {
                    let mut json_map = Map::new();
                    for (key, value) in labels.iter().zip(values.into_iter()) {
                        json_map.insert(key.to_string(), value.try_into()?);
                    }

                    JsonValue::Object(json_map).to_string()
                }
                DataRow::Map(hash_map) => {
                    let mut json_map = Map::new();
                    for (key, value) in hash_map {
                        json_map.insert(key.to_string(), value.try_into()?);
                    }

                    JsonValue::Object(json_map).to_string()
                }
            };
            writeln!(file, "{json_string}").map_storage_err()?;
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        let prev_rows = self.scan_data(table_name)?;
        rows.sort_by(|(key_a, _), (key_b, _)| {
            key_a.to_cmp_be_bytes().cmp(&key_b.to_cmp_be_bytes())
        });
        let rows = rows.into_iter();

        let sort_merge = SortMerge::new(prev_rows, rows);
        let merged = sort_merge.collect::<Result<Vec<_>>>()?;

        let table_path = self.data_path(table_name);
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, merged).await
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let prev_rows = self.scan_data(table_name)?;
        let rows = prev_rows
            .filter_map(|result| {
                result
                    .map(|(key, data_row)| {
                        let preservable = !keys.iter().any(|target_key| target_key == &key);

                        preservable.then_some(data_row)
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        let table_path = self.data_path(table_name);
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, rows).await
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

        let (left_key, right_key) = match (left, right) {
            (Some(Ok((left_key, _))), Some((right_key, _))) => (left_key, right_key),
            (Some(_), _) => {
                return self.left_rows.next().map(|item| Ok(item?.1));
            }
            (None, Some(_)) => {
                return self.right_rows.next().map(|item| item.1).map(Ok);
            }
            (None, None) => {
                return None;
            }
        };

        match left_key.to_cmp_be_bytes().cmp(&right_key.to_cmp_be_bytes()) {
            Ordering::Less => self.left_rows.next(),
            Ordering::Greater => self.right_rows.next().map(Ok),
            Ordering::Equal => {
                self.left_rows.next();
                self.right_rows.next().map(Ok)
            }
        }
        .map(|item| Ok(item?.1))
    }
}
