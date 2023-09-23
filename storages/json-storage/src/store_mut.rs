use {
    crate::{
        error::{JsonStorageError, OptionExt, ResultExt},
        JsonStorage,
    },
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, StoreMut},
    },
    serde_json::{to_string_pretty, Map, Value as JsonValue},
    std::{
        fs::{remove_file, File, OpenOptions},
        io::Write,
        {cmp::Ordering, iter::Peekable, vec::IntoIter},
    },
};

#[async_trait(?Send)]
impl StoreMut for JsonStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_path = self.jsonl_path(schema.table_name.as_str());
        File::create(data_path).map_storage_err()?;

        if schema.column_defs.is_some() {
            let schema_path = self.schema_path(schema.table_name.as_str());
            let ddl = schema.to_ddl();
            let mut file = File::create(schema_path).map_storage_err()?;

            file.write_all(ddl.as_bytes()).map_storage_err()?;
        }

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let json_path = self.json_path(table_name);
        let jsonl_path = self.jsonl_path(table_name);

        match (json_path.exists(), jsonl_path.exists()) {
            (true, false) => remove_file(json_path).map_storage_err()?,
            (false, true) => remove_file(jsonl_path).map_storage_err()?,
            _ => {}
        }

        let schema_path = self.schema_path(table_name);
        if schema_path.exists() {
            remove_file(schema_path).map_storage_err()?;
        }

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        let json_path = self.json_path(table_name);
        if json_path.exists() {
            let (prev_rows, schema) = self.scan_data(table_name)?;

            let rows = prev_rows
                .map(|item| Ok(item?.1))
                .chain(rows.into_iter().map(Ok))
                .collect::<Result<Vec<_>>>()?;

            let file = File::create(&json_path).map_storage_err()?;

            self.write(schema, rows, file, true)
        } else {
            let schema = self
                .fetch_schema(table_name)?
                .map_storage_err(JsonStorageError::TableDoesNotExist)?;

            let file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(self.jsonl_path(&schema.table_name))
                .map_storage_err()?;

            self.write(schema, rows, file, false)
        }
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        let (prev_rows, schema) = self.scan_data(table_name)?;
        rows.sort_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));

        let sort_merge = SortMerge::new(prev_rows, rows.into_iter());
        let merged = sort_merge.collect::<Result<Vec<_>>>()?;

        self.rewrite(schema, merged)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let (prev_rows, schema) = self.scan_data(table_name)?;
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

        self.rewrite(schema, rows)
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

impl JsonStorage {
    fn rewrite(&mut self, schema: Schema, rows: Vec<DataRow>) -> Result<()> {
        let json_path = self.json_path(&schema.table_name);
        let (path, is_json) = match json_path.exists() {
            true => (json_path, true),
            false => {
                let jsonl_path = self.jsonl_path(&schema.table_name);

                (jsonl_path, false)
            }
        };
        let file = File::create(path).map_storage_err()?;

        self.write(schema, rows, file, is_json)
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
                    .zip(values)
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
