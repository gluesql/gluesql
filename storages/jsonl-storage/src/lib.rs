mod alter_table;
mod error;
mod index;
mod store;
mod store_mut;
mod transaction;

use {
    error::{JsonlStorageError, OptionExt, ResultExt},
    gluesql_core::{
        chrono::NaiveDateTime,
        data::{HashMapJsonExt, Schema},
        prelude::Key,
        result::Result,
        store::{DataRow, RowIter},
    },
    std::{
        collections::HashMap,
        fs::{self, File},
        io::{self, BufRead, Read},
        path::{Path, PathBuf},
    },
};

#[derive(Debug)]
pub struct JsonlStorage {
    pub path: PathBuf,
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
                                .map_storage_err(
                                    JsonlStorageError::ColumnDoesNotExist(column_def.name.clone())
                                        .to_string(),
                                )?
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

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[cfg(test)]
mod tests {
    use {
        crate::*,
        gluesql_core::{
            data::{Interval, SchemaParseError, ValueError},
            prelude::{
                Glue, {Payload, Value},
            },
            result::Error,
        },
        std::net::{IpAddr, Ipv4Addr},
        uuid::Uuid as UUID,
    };

    #[test]
    fn jsonl_storage_test() {
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

        macro_rules! date {
            ($date: expr) => {
                $date.parse().unwrap()
            };
        }
        let parse_uuid = |v| UUID::parse_str(v).unwrap().as_u128();
        let bytea = |v| hex::decode(v).unwrap();
        let m = |s: &str| Value::parse_json_map(s).unwrap();
        let l = |s: &str| Value::parse_json_list(s).unwrap();

        let actual = glue.execute("SELECT * FROM Schema").unwrap();
        let actual = actual.get(0).unwrap();
        let expected = Payload::Select {
            labels: [
                "boolean",
                "int8",
                "int16",
                "int32",
                "int64",
                "int128",
                "uint8",
                "text",
                "bytea",
                "inet",
                "date",
                "timestamp",
                "time",
                "interval",
                "uuid",
                "map",
                "list",
            ]
            .into_iter()
            .map(ToOwned::to_owned)
            .collect(),
            rows: vec![vec![
                Value::Bool(true),
                Value::I8(93),
                Value::I16(18550),
                Value::I32(1726758346),
                Value::I64(5724242792803127000),
                Value::I128(26574198945213317000),
                Value::U8(159),
                Value::Str("Blake Funk".to_owned()),
                Value::Bytea(bytea("f5DD42C123")),
                Value::Inet(IpAddr::V4(Ipv4Addr::new(14, 232, 199, 19))),
                Value::Date(date!("2022-08-14")),
                Value::Timestamp(date!("2023-02-10T00:03:21")),
                Value::Time(date!("10:26:32")),
                Value::Interval(Interval::hours(35)),
                Value::Uuid(parse_uuid("54afd535-13ef-429a-8555-8025afe4d5e1")),
                m(r#"{"age": 55, "city": "East Nicofurt"}"#),
                l(r#"["fuchsia", "olive", "tan"]"#),
            ]],
        };
        assert_eq!(actual, &expected);

        let actual = glue.execute("SELECT * FROM WrongFormat");
        let expected = Err(ValueError::InvalidJsonString("{".to_owned()).into());

        assert_eq!(actual, expected);

        let actual = glue.execute("SELECT * FROM WrongSchema");
        let expected = Err(Error::Schema(SchemaParseError::CannotParseDDL));

        assert_eq!(actual, expected);
    }
}
