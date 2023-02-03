mod alter_table;
mod index;
mod transaction;

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{HashMapJsonExt, Schema},
        prelude::{parse, translate, Key},
        result::{Error, Result},
        store::{DataRow, RowIter, Store},
        {chrono::NaiveDateTime, store::StoreMut},
    },
    iter_enum::Iterator,
    serde_json::Value as JsonValue,
    std::{
        collections::HashMap,
        fmt,
        fs::{self, remove_file, File, OpenOptions},
        io::{self, prelude::*, BufRead},
        path::{Path, PathBuf},
    },
    utils::HashMapExt,
};

#[derive(Debug)]
pub struct JsonlStorage {
    pub path: PathBuf,
}

impl Default for JsonlStorage {
    fn default() -> Self {
        JsonlStorage {
            path: PathBuf::from("data"),
        }
    }
}

trait ResultExt<T, E: ToString> {
    fn map_storage_err(self) -> Result<T, Error>;
}

impl<T, E: ToString> ResultExt<T, E> for std::result::Result<T, E> {
    fn map_storage_err(self) -> Result<T, Error> {
        self.map_err(|e| e.to_string()).map_err(Error::StorageMsg)
    }
}

trait OptionExt<T> {
    fn map_storage_err(self, payload: String) -> Result<T, Error>;
}

impl<T> OptionExt<T> for std::option::Option<T> {
    fn map_storage_err(self, payload: String) -> Result<T, Error> {
        self.ok_or_else(|| payload.to_string())
            .map_err(Error::StorageMsg)
    }
}

enum JsonlStorageError {
    FileNotFound,
    CannotConvertToString,
    TableDoesNotExist,
    ColumnDoesNotExist,
}

impl fmt::Display for JsonlStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let payload = match self {
            JsonlStorageError::FileNotFound => "file not found",
            JsonlStorageError::CannotConvertToString => "cannot convert to string",
            JsonlStorageError::TableDoesNotExist => "table does not exist",
            JsonlStorageError::ColumnDoesNotExist => "column does not exist",
        };

        write!(f, "{}", payload)
    }
}

impl JsonlStorage {
    // fn fetch_all_schemas(&self) -> Result<HashMap<String, Schema>> {
    //     let paths = fs::read_dir(self.path).map_storage_err()?;
    //     paths
    //         .filter(|result| {
    //             result
    //                 .as_ref()
    //                 .map(|dir_entry| {
    //                     dir_entry
    //                         .path()
    //                         .extension()
    //                         .map(|os_str| os_str.to_str() == Some("jsonl"))
    //                         .unwrap_or(false)
    //                 })
    //                 .unwrap_or(false)
    //         })
    //         .map(|result| -> Result<_> {
    //             let path = result.map_storage_err()?.path();
    //             let table_name = path
    //                 .file_stem()
    //                 .map_storage_err(JsonlStorageError::FileNotFound.to_string())?
    //                 .to_str()
    //                 .map_storage_err(JsonlStorageError::CannotConvertToString.to_string())?
    //                 .to_owned();

    //             let jsonl_table = JsonlStorage::new_table(table_name.clone());

    //             Ok((table_name, jsonl_table))
    //         })
    //         .collect::<Result<HashMap<String, Schema>>>()
    // }

    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path);

        Ok(Self { path })
    }

    fn get_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if !self.data_path(table_name).exists() {
            return Ok(None);
        };

        let schema_path = self.schema_path(table_name);
        // todo: try then_some
        let column_defs = match schema_path.exists() {
            true => {
                let mut file = File::open(schema_path).map_storage_err()?;
                let mut ddl = String::new();
                file.read_to_string(&mut ddl).map_storage_err()?;

                let parsed = parse(ddl)?.into_iter().next().unwrap();
                let statement = translate(&parsed)?;

                let column_defs = match statement {
                    gluesql_core::ast::Statement::CreateTable { columns, .. } => columns,
                    _ => todo!(),
                };

                Ok::<_, Error>(Some(column_defs))
            }
            false => Ok(None),
        }?;

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            created: NaiveDateTime::default(),
        }))
    }

    fn data_path(&self, table_name: &str) -> PathBuf {
        let path = self.path_by(table_name, "jsonl");

        PathBuf::from(path)
    }

    fn schema_path(&self, table_name: &str) -> PathBuf {
        let path = self.path_by(table_name, "sql");

        PathBuf::from(path)
    }

    fn path_by(&self, table_name: &str, extension: &str) -> String {
        // let schema = self
        //     .tables
        //     .get(table_name)
        //     .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?;
        let path = format!("{}/{}.{extension}", self.path.display(), table_name);

        path
    }

    fn write_schema(&self, schema: &Schema) -> Result<()> {
        // let schema_path = format!("{}/{}.sql", self.path.display(), schema.table_name);
        let schema_path = self.schema_path(schema.table_name.as_str());
        let ddl = schema.clone().to_ddl();
        let mut file = File::create(schema_path).map_storage_err()?;
        write!(file, "{ddl}").map_storage_err()?;

        Ok(())
    }

    fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        // let schema = self
        //     .tables
        //     .get(table_name)
        //     .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?
        //     .to_owned();
        let schema = self
            .get_schema(table_name)?
            .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?;
        let data_path = self.data_path(table_name);
        let lines = read_lines(data_path).map_storage_err()?;
        let row_iter = lines.enumerate().map(move |(key, line)| -> Result<_> {
            let hash_map = HashMap::parse_json_object(&line.map_storage_err()?)?;
            let data_row = match schema.clone().column_defs {
                Some(column_defs) => {
                    let values = column_defs
                        .iter()
                        .map(|column_def| -> Result<_> {
                            let value = hash_map
                                .get(&column_def.name)
                                .map_storage_err(JsonlStorageError::ColumnDoesNotExist.to_string())?
                                .clone();
                            let data_type = value.get_type();
                            match data_type {
                                Some(data_type) => match data_type == column_def.data_type {
                                    true => Ok(value),
                                    false => value.cast(&column_def.data_type),
                                },
                                None => Ok(value),
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    DataRow::Vec(values)
                }
                None => DataRow::Map(hash_map),
            };
            let key = Key::I64((key + 1).try_into().map_storage_err()?);

            Ok((key, data_row))
        });

        Ok(Box::new(row_iter))
    }
}

#[async_trait(?Send)]
impl Store for JsonlStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.get_schema(table_name)
        // Ok(Some(self.get_schema(table_name)))
        // Ok(self.tables.get(table_name).map(ToOwned::to_owned))
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let paths = fs::read_dir(self.path.clone()).map_storage_err()?;
        paths
            .filter(|result| {
                result
                    .as_ref()
                    .map(|dir_entry| {
                        dir_entry
                            .path()
                            .extension()
                            .map(|os_str| os_str.to_str() == Some("jsonl"))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
            })
            .map(|result| -> Result<_> {
                let path = result.map_storage_err()?.path();
                let table_name = path
                    .file_stem()
                    .map_storage_err(JsonlStorageError::FileNotFound.to_string())?
                    .to_str()
                    .map_storage_err(JsonlStorageError::CannotConvertToString.to_string())?
                    .to_owned();

                // todo! check and add schema
                // Ok(JsonlStorage::get_schema(table_name.clone()))
                self.get_schema(table_name.as_str())?
                    .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())

                // Ok((table_name, jsonl_table))
            })
            .collect::<Result<Vec<Schema>>>()
        // let mut vec = self
        //     .tables
        //     .iter()
        //     .map(|table| table.1.to_owned())
        //     .collect::<Vec<_>>();
        // // vec.sort();
        // vec.sort_by(|key_a, key_b| key_a.table_name.cmp(&key_b.table_name));

        // Ok(vec)
    }

    async fn fetch_data(&self, table_name: &str, target: &Key) -> Result<Option<DataRow>> {
        let row = self.scan_data(table_name)?.find_map(|result| {
            result
                .map(|(key, row)| (&key == target).then_some(row))
                .unwrap_or(None)
        });

        Ok(row)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        self.scan_data(table_name)
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[async_trait(?Send)]
impl StoreMut for JsonlStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_path = self.data_path(schema.table_name.as_str());
        File::create(data_path).map_storage_err()?;

        if schema.column_defs.is_some() {
            self.write_schema(schema)?
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
        #[derive(Iterator)]
        enum JsonIter<I1, I2> {
            Map(I1),
            Vec(I2),
        }

        let schema = self
            .get_schema(table_name)?
            .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?;
        let table_path = JsonlStorage::data_path(self, table_name);

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(table_path)
            .map_storage_err()?;

        for row in rows {
            let json_string = match row {
                DataRow::Map(hash_map) => JsonIter::Map(hash_map.into_iter()),
                DataRow::Vec(values) => {
                    match &schema.column_defs {
                        Some(column_defs) => {
                            // todo! validate columns?
                            JsonIter::Vec(
                                column_defs
                                    .iter()
                                    .map(|column_def| column_def.name.clone())
                                    .zip(values.into_iter()),
                            )
                        }
                        None => break, // todo! looks like unreachable
                    }
                }
            }
            .map(|(key, value)| {
                let value = JsonValue::try_from(value)?.to_string();

                Ok(format!("\"{key}\": {value}"))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");

            writeln!(file, "{{{json_string}}}").map_storage_err()?;
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
        let prev_rows = self.scan_data(table_name)?;

        // todo! impl without sort + vector.zip
        let prev_rows = prev_rows.collect::<Result<HashMap<Key, DataRow>>>()?;

        let rows = prev_rows.concat(rows.into_iter());
        let mut rows = rows.into_iter().collect::<Vec<_>>();

        rows.sort_by(|(key_a, _), (key_b, _)| {
            key_a
                .partial_cmp(key_b)
                .unwrap_or(std::cmp::Ordering::Equal) // todo! okay to be equal?
        });

        let rows = rows.into_iter().map(|(_, data_row)| data_row).collect();

        let table_path = self.data_path(table_name);
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, rows).await
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
                    .unwrap_or(None)
                // todo! how not to ignore error?
                // can remove result from RowIter?
            })
            .collect::<Vec<_>>();

        let table_path = self.data_path(table_name);
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, rows).await
    }
}

#[test]
fn jsonl_storage_test() {
    use crate::*;
    // use futures::executor::block_on;
    use gluesql_core::prelude::Glue;
    use gluesql_core::prelude::{Payload, Value};

    let path = "./samples/";
    let jsonl_storage = JsonlStorage::new(path).unwrap();
    let mut glue = Glue::new(jsonl_storage);

    let actual = glue.execute("SELECT * FROM Schemaless").unwrap();
    let actual = actual.get(0).unwrap();
    let expected = Payload::SelectMap(vec![
        [("id".to_owned(), Value::I64(1))].into_iter().collect(),
        [("name".to_owned(), Value::Str("Glue".to_owned()))]
            .into_iter()
            .collect(),
        [
            ("id".to_owned(), Value::I64(3)),
            ("name".to_owned(), Value::Str("SQL".to_owned())),
        ]
        .into_iter()
        .collect(),
    ]);
    assert_eq!(actual, &expected);

    // let actual = glue.execute("SELECT * FROM Schema").unwrap();
    // let actual = actual.get(0).unwrap();
    // let expected = Payload::Select {
    //     labels: ["id", "name"].into_iter().map(ToOwned::to_owned).collect(),
    //     rows: vec![
    //         vec![Value::I64(1), Value::Str("Glue".to_owned())],
    //         vec![Value::I64(2), Value::Str("SQL".to_owned())],
    //     ],
    // };
    // assert_eq!(actual, &expected);

    // let table_name = "Items".to_string();
    // let schema = Schema {
    //     table_name: table_name.clone(),
    //     column_defs: None,
    //     indexes: Vec::new(),
    //     created: NaiveDateTime::default(),
    // };
    // block_on(async {
    //     jsonl_storage.insert_schema(&schema);
    //     let actual = jsonl_storage
    //         .fetch_schema(&table_name)
    //         .await
    //         .unwrap()
    //         .unwrap();
    //     assert_eq!(actual, schema);
    // });
}
