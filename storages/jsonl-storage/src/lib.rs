mod alter_table;
mod index;
mod transaction;

use {
    async_trait::async_trait,
    gluesql_core::{
        data::{HashMapJsonExt, Schema},
        prelude::Key,
        result::{Error, Result},
        store::{DataRow, RowIter, Store},
        {chrono::NaiveDateTime, store::StoreMut},
    },
    serde_json::{Map, Value as JsonValue},
    std::{
        cmp::Ordering,
        collections::HashMap,
        fmt,
        fs::{self, remove_file, File, OpenOptions},
        io::{self, prelude::*, BufRead},
        path::{Path, PathBuf},
    },
};

#[derive(Debug)]
pub struct JsonlStorage {
    pub path: PathBuf,
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
    TableDoesNotExist,
    ColumnDoesNotExist,
}

impl fmt::Display for JsonlStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let payload = match self {
            JsonlStorageError::FileNotFound => "file not found".to_owned(),
            JsonlStorageError::TableDoesNotExist => "table does not exist".to_owned(),
            JsonlStorageError::ColumnDoesNotExist => "column does not exist".to_owned(),
        };

        write!(f, "{}", payload)
    }
}

impl JsonlStorage {
    pub fn new(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_storage_err()?;
        let path = PathBuf::from(path);

        Ok(Self { path })
    }

    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        if !self.data_path(table_name).exists() {
            return Ok(None);
        };

        let schema_path = self.schema_path(table_name);
        let column_defs = match schema_path.exists() {
            true => {
                let mut file = File::open(&schema_path).map_storage_err()?;
                let mut ddl = String::new();
                file.read_to_string(&mut ddl).map_storage_err()?;

                Schema::from_ddl(&ddl).map(|schema| schema.column_defs)
            }
            false => Ok(None),
        }?;

        Ok(Some(Schema {
            table_name: table_name.to_owned(),
            column_defs,
            indexes: vec![],
            created: NaiveDateTime::default(),
            engine: None,
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
        let path = format!("{}/{}.{extension}", self.path.display(), table_name);

        path
    }

    fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let schema = self
            .fetch_schema(table_name)?
            .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?;
        let data_path = self.data_path(table_name);
        let lines = read_lines(data_path).map_storage_err()?;
        let row_iter = lines.enumerate().map(move |(key, line)| -> Result<_> {
            let hash_map = HashMap::parse_json_object(&line.map_storage_err()?)?;
            let data_row = match &schema.column_defs {
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
        self.fetch_schema(table_name)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let paths = fs::read_dir(&self.path).map_storage_err()?;
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
                    .map_storage_err(JsonlStorageError::FileNotFound.to_string())?
                    .to_owned();

                self.fetch_schema(table_name.as_str())?
                    .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())
            })
            .collect::<Result<Vec<Schema>>>()
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
            .map_storage_err(JsonlStorageError::TableDoesNotExist.to_string())?;
        let table_path = JsonlStorage::data_path(self, table_name);

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
        let mut prev_rows = self.scan_data(table_name)?.peekable();
        rows.sort_by(|(key_a, _), (key_b, _)| {
            key_a
                .partial_cmp(key_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut rows = rows.into_iter().peekable();
        let mut bucket: Vec<DataRow> = Vec::new();

        while prev_rows.peek().is_some() | rows.peek().is_some() {
            match (prev_rows.peek(), rows.peek()) {
                (Some(result), Some((key, row))) => {
                    let (prev_key, prev_row) = result.as_ref().unwrap();

                    match prev_key.to_cmp_be_bytes().cmp(&key.to_cmp_be_bytes()) {
                        Ordering::Less => {
                            bucket.push(prev_row.to_owned());
                            prev_rows.next();
                        }
                        Ordering::Greater => {
                            bucket.push(row.to_owned());
                            rows.next();
                        }
                        Ordering::Equal => {
                            bucket.push(row.to_owned());
                            rows.next();
                            prev_rows.next();
                        }
                    }
                }
                (Some(_), None) => {
                    let prev_rows = prev_rows
                        .map(|result| {
                            let (_, prev_row) = result?;
                            Ok(prev_row)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    bucket.extend(prev_rows);

                    break;
                }
                (None, Some(_)) => {
                    let rows = rows.map(|(_, row)| row).collect::<Vec<_>>();
                    bucket.extend(rows);

                    break;
                }
                (None, None) => {}
            }
        }

        let table_path = self.data_path(table_name);
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, bucket).await
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
            })
            .collect::<Vec<_>>();

        let table_path = self.data_path(table_name);
        File::create(&table_path).map_storage_err()?;

        self.append_data(table_name, rows).await
    }
}

#[test]
fn jsonl_storage_test() {
    use {
        crate::*,
        gluesql_core::{
            data::{SchemaParseError, ValueError},
            prelude::{
                Glue, {Payload, Value},
            },
        },
    };

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

    let actual = glue.execute("SELECT * FROM Schema").unwrap();
    let actual = actual.get(0).unwrap();
    let expected = Payload::Select {
        labels: ["id", "name"].into_iter().map(ToOwned::to_owned).collect(),
        rows: vec![
            vec![Value::I64(1), Value::Str("Glue".to_owned())],
            vec![Value::I64(2), Value::Str("SQL".to_owned())],
        ],
    };
    assert_eq!(actual, &expected);

    let actual = glue.execute("SELECT * FROM WrongFormat");
    let expected = Err(ValueError::InvalidJsonString("{".to_owned()).into());

    assert_eq!(actual, expected);

    let actual = glue.execute("SELECT * FROM WrongSchema");
    let expected = Err(Error::Schema(SchemaParseError::CannotParseDDL));

    assert_eq!(actual, expected);
}
