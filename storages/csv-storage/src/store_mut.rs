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
        cmp::Ordering,
        collections::BTreeSet,
        fs::{remove_file, rename, File, OpenOptions},
        io::Write,
        iter::Peekable,
        vec::IntoIter,
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
        let (columns, prev_rows) = self.scan_data(table_name)?;

        if columns.is_some() {
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
            let rows = prev_rows
                .map(|item| item.map(|(_, row)| row))
                .chain(rows.into_iter().map(Ok));

            self.write(table_name, columns, rows)
        }
    }

    async fn insert_data(&mut self, table_name: &str, mut rows: Vec<(Key, DataRow)>) -> Result<()> {
        let (columns, prev_rows) = self.scan_data(table_name)?;

        rows.sort_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));

        let merged = SortMerge::new(prev_rows, rows.into_iter());

        self.write(table_name, columns, merged)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let (columns, prev_rows) = self.scan_data(table_name)?;
        let rows = prev_rows.filter_map(|item| {
            let (key, data_row) = match item {
                Ok(item) => item,
                Err(e) => return Some(Err(e)),
            };

            keys.iter()
                .all(|target_key| target_key != &key)
                .then_some(Ok(data_row))
        });

        self.write(table_name, columns, rows)
    }
}

impl CsvStorage {
    fn write<T: Iterator<Item = Result<DataRow>>>(
        &self,
        table_name: &str,
        columns: Option<Vec<String>>,
        rows: T,
    ) -> Result<()> {
        let tmp_data_path = self.tmp_data_path(table_name);
        let mut data_wtr = File::create(&tmp_data_path)
            .map_storage_err()
            .map(Writer::from_writer)?;

        if let Some(columns) = columns {
            data_wtr.write_record(&columns).map_storage_err()?;

            for row in rows {
                let row = convert(row?)?;

                data_wtr.write_record(&row).map_storage_err()?;
            }
        } else {
            let tmp_types_path = self.tmp_types_path(table_name);
            let mut types_wtr = File::create(&tmp_types_path)
                .map(Writer::from_writer)
                .map_storage_err()?;

            let mut columns = BTreeSet::new();
            let rows = rows
                .map(|row| match row? {
                    DataRow::Vec(_) => {
                        Err(CsvStorageError::UnreachableVecTypeDataRowTypeFound.into())
                    }
                    DataRow::Map(values) => Ok(values),
                })
                .collect::<Result<Vec<_>>>()?;

            for row in &rows {
                columns.extend(row.keys());
            }

            data_wtr.write_record(&columns).map_storage_err()?;
            types_wtr.write_record(&columns).map_storage_err()?;

            for row in &rows {
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
            }

            rename(tmp_types_path, self.types_path(table_name)).map_storage_err()?
        }

        rename(tmp_data_path, self.data_path(table_name)).map_storage_err()
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
