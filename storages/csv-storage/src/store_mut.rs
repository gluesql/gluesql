use {
    crate::{error::ResultExt, CsvStorage},
    async_trait::async_trait,
    gluesql_core::{
        data::{Key, Schema, Value},
        error::{Error, Result},
        store::{DataRow, StoreMut},
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        fs::{remove_file, File, OpenOptions},
        io::Write,
        {cmp::Ordering, iter::Peekable, vec::IntoIter},
    },
};

#[async_trait(?Send)]
impl StoreMut for CsvStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let schema_path = self.schema_path(schema.table_name.as_str());
        let ddl = schema.to_ddl();
        let mut file = File::create(schema_path).map_storage_err()?;
        file.write_all(ddl.as_bytes()).map_storage_err()?;

        let column_defs = match &schema.column_defs {
            Some(column_defs) => column_defs,
            None => {
                return Ok(());
            }
        };

        let columns = column_defs
            .iter()
            .map(|column_def| column_def.name.as_str())
            .collect::<Vec<&str>>();
        let data_path = self.data_path(schema.table_name.as_str());
        let file = File::create(data_path).map_storage_err()?;
        let mut wtr = csv::Writer::from_writer(file);
        wtr.write_record(&columns).map_storage_err()?;

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
        // let schema_path = self.schema_path(table_name);
        let data_path = self.data_path(table_name);

        // if schema_path does not exist -> insert_data without key? -> key to enumerate

        let schema = self.fetch_schema(table_name)?;
        match schema {
            Some(Schema {
                column_defs: Some(_),
                ..
            }) if data_path.exists() => {
                println!("path exists: {table_name}");

                let file = OpenOptions::new()
                    .append(true)
                    .open(data_path)
                    .map_storage_err()?;

                let mut wtr = csv::Writer::from_writer(file);

                for row in rows {
                    let row = convert(row);

                    wtr.write_record(&row).map_storage_err()?;
                }

                Ok(())
            }
            _ => {
                let rows = rows
                    .into_iter()
                    .enumerate()
                    .map(|(i, row)| (Key::U64(i as u64), row))
                    .collect();

                self.insert_data(table_name, rows).await
            }
        }

        /*
        match data_path.exists() {
            true => {
            }
            false => {
                self.insert_data(table_name, rows).await
                /*
                // this means schemaless?
                panic!("append_data append_data");
                println!("path does not exist: {table_name}");

                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(data_path)
                    .map_storage_err()?;

                let (columns, _) = self.scan_data(table_name)?;

                let wtr = csv::Writer::from_writer(file);
                write(wtr, columns, rows)
                */
            }
        }
        */
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        println!("insert_data called for {table_name}");

        let (columns, prev_rows) = self.scan_data(table_name)?;

        rows.sort_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));

        let sort_merge = SortMerge::new(prev_rows, rows.into_iter());
        let merged = sort_merge.collect::<Result<Vec<_>>>()?;

        let file = File::create(self.data_path(table_name)).map_storage_err()?;
        let wtr = csv::Writer::from_writer(file);
        write(wtr, columns, merged)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let (columns, prev_rows) = self.scan_data(table_name)?;
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

        let file = File::create(self.data_path(table_name)).map_storage_err()?;
        let wtr = csv::Writer::from_writer(file);

        write(wtr, columns, rows)
    }
}

fn write(
    mut wtr: csv::Writer<File>,
    columns: Option<Vec<String>>,
    rows: Vec<DataRow>,
) -> Result<()> {
    if let Some(columns) = columns {
        wtr.write_record(&columns).map_storage_err()?;

        for row in rows {
            let row = convert(row);

            wtr.write_record(&row).map_storage_err()?;
        }

        return Ok(());
    }

    // schemaless
    let mut columns = BTreeSet::new();
    let rows = rows
        .into_iter()
        .map(|row| match row {
            DataRow::Vec(_) => Err(Error::StorageMsg("something error".to_owned())),
            DataRow::Map(values) => Ok(values),
        })
        .collect::<Result<Vec<_>>>()?;

    for row in &rows {
        columns.extend(row.keys());
    }

    wtr.write_record(&columns).map_storage_err()?;

    for row in &rows {
        let row = columns
            .iter()
            .map(|key| {
                let value = row.get(key.as_str()).unwrap_or(&Value::Null);

                String::from(value)
            })
            .collect::<Vec<_>>();

        wtr.write_record(&row).map_storage_err()?;
    }

    /*
    let mut rows = rows.into_iter().map(|row| match row {
        DataRow::Vec(values) => {
            columns.extend(values.into_iter().map(String::from));

            Ok(values)
        }
        DataRow::Map(values) => {
            columns.extend(values.keys().map(String::from));

            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
    });

    let columns = columns.unwrap_or_default();
    */

    Ok(())
}

fn convert(data_row: DataRow) -> Vec<String> {
    match data_row {
        DataRow::Vec(values) => values.into_iter().map(String::from).collect(),
        DataRow::Map(values) => BTreeMap::from_iter(values.into_iter())
            .into_values()
            .map(String::from)
            .collect(),
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
