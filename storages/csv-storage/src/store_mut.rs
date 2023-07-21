use {
    crate::{
        error::{CsvStorageError, ResultExt},
        CsvStorage,
    },
    async_trait::async_trait,
    csv::Writer,
    gluesql_core::{
        data::{Key, Schema},
        error::Result,
        store::{DataRow, StoreMut},
    },
    std::{
        collections::BTreeSet,
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

        File::create(data_path)
            .map_storage_err()
            .map(Writer::from_writer)?
            .write_record(&columns)
            .map_storage_err()
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let data_path = self.data_path(table_name);
        if data_path.exists() {
            remove_file(data_path).map_storage_err()?;
        }

        let types_path = self.types_path(table_name);
        if types_path.exists() {
            remove_file(types_path).map_storage_err()?;
        }

        let schema_path = self.schema_path(table_name);
        if schema_path.exists() {
            remove_file(schema_path).map_storage_err()?;
        }

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
        if self.has_columns(table_name)? {
            let data_path = self.data_path(table_name);
            let mut wtr = OpenOptions::new()
                .append(true)
                .open(data_path)
                .map_storage_err()
                .map(Writer::from_writer)?;

            for row in rows {
                let row = convert(row)?;

                wtr.write_record(&row).map_storage_err()?;
            }

            Ok(())
        } else {
            let rows = rows
                .into_iter()
                .enumerate()
                .map(|(i, row)| (Key::U64(i as u64), row))
                .collect();

            self.insert_data(table_name, rows).await
        }
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        let (columns, prev_rows) = self.scan_data(table_name)?;

        rows.sort_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));

        let sort_merge = SortMerge::new(prev_rows, rows.into_iter());
        let merged = sort_merge.collect::<Result<Vec<_>>>()?;

        self.write(table_name, columns, merged)
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

        self.write(table_name, columns, rows)
    }
}

impl CsvStorage {
    fn write(
        &self,
        table_name: &str,
        columns: Option<Vec<String>>,
        rows: Vec<DataRow>,
    ) -> Result<()> {
        let mut data_wtr = File::create(self.data_path(table_name))
            .map_storage_err()
            .map(Writer::from_writer)?;

        if let Some(columns) = columns {
            data_wtr.write_record(&columns).map_storage_err()?;

            for row in rows {
                let row = convert(row)?;

                data_wtr.write_record(&row).map_storage_err()?;
            }

            return Ok(());
        }

        let mut types_wtr = columns
            .is_none()
            .then(|| {
                File::create(self.types_path(table_name))
                    .map_storage_err()
                    .map(Writer::from_writer)
            })
            .transpose()?;

        // schemaless
        let mut columns = BTreeSet::new();
        let rows = rows
            .into_iter()
            .map(|row| match row {
                DataRow::Vec(_) => Err(CsvStorageError::UnreachableVecTypeDataRowTypeFound.into()),
                DataRow::Map(values) => Ok(values),
            })
            .collect::<Result<Vec<_>>>()?;

        for row in &rows {
            columns.extend(row.keys());
        }

        data_wtr.write_record(&columns).map_storage_err()?;
        if let Some(types_wtr) = &mut types_wtr {
            types_wtr.write_record(&columns).map_storage_err()?;
        }

        for row in &rows {
            if let Some(types_wtr) = &mut types_wtr {
                let (row, data_types): (Vec<_>, Vec<_>) = columns
                    .iter()
                    .map(|key| {
                        row.get(key.as_str())
                            .map(|value| {
                                let data_type = value
                                    .get_type()
                                    .map(|t| t.to_string())
                                    .unwrap_or("NULL".to_owned());

                                (String::from(value), data_type)
                            })
                            .unwrap_or(("NULL".to_owned(), "".to_owned()))
                    })
                    .unzip();

                data_wtr.write_record(&row).map_storage_err()?;
                types_wtr.write_record(&data_types).map_storage_err()?;
            } else {
                let row = columns
                    .iter()
                    .map(|key| {
                        row.get(key.as_str())
                            .map(String::from)
                            .unwrap_or("NULL".to_owned())
                    })
                    .collect::<Vec<_>>();

                data_wtr.write_record(&row).map_storage_err()?;
            }
        }

        Ok(())
    }
}

fn convert(data_row: DataRow) -> Result<Vec<String>> {
    match data_row {
        DataRow::Vec(values) => Ok(values.into_iter().map(String::from).collect()),
        DataRow::Map(_) => Err(CsvStorageError::UnreachableMapTypeDataRowFound.into()),
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
