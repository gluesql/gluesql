#[cfg(feature = "sled-storage")]
use crate::{execute, storages::SledStorage, ExecuteError, Payload, Query, Result, Row};
#[cfg(feature = "sled-storage")]
use futures::executor::block_on;

#[cfg(feature = "sled-storage")]
pub struct Glue {
    storage: Option<SledStorage>,
}

#[cfg(feature = "sled-storage")]
impl Glue {
    pub fn new(storage: SledStorage) -> Self {
        let storage = Some(storage);

        Self { storage }
    }

    pub fn execute(&mut self, query: &Query) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match block_on(execute(storage, query)) {
            Ok((storage, payload)) => {
                self.storage = Some(storage);

                Ok(payload)
            }
            Err((storage, error)) => {
                self.storage = Some(storage);

                Err(error)
            }
        }
    }

    pub fn select_as_string(&mut self, query: &Query) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        // TODO: Make this more efficient by converting earlier
        match self.execute(query) {
            Ok(Payload::Select { labels, rows }) => Ok((
                labels,
                rows.into_iter()
                    .map(|row| row.0.into_iter().map(|value| value.into()).collect())
                    .collect(),
            )),
            _ => Err(ExecuteError::QueryNotSupported.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{parse_sql::parse, Glue, Payload, Row, SledStorage, Value};
    #[test]
    fn eq() {
        let path = "test";
        std::fs::remove_dir_all(&path);
        let sled = SledStorage::new(path).unwrap();
        let mut glue = Glue::new(sled);
        glue.execute(
            &parse("CREATE TABLE api_test (id INTEGER PRIMARY KEY, name TEXT)").unwrap()[0],
        );
        assert_eq!(
            glue.execute(
                &parse("INSERT INTO api_test (id, name) VALUES (1, 'test1'), (2, 'test2')")
                    .unwrap()[0]
            ),
            Ok(Payload::Insert(2))
        );

        assert_eq!(
            glue.execute(&parse("SELECT * FROM api_test").unwrap()[0]),
            Ok(Payload::Select {
                labels: vec![String::from("id"), String::from("name")],
                rows: vec![
                    Row(vec![Value::I64(1), Value::Str(String::from("test1"))]),
                    Row(vec![Value::I64(2), Value::Str(String::from("test2"))])
                ]
            })
        );
        assert_eq!(
            glue.select_as_string(&parse("SELECT * FROM api_test").unwrap()[0]),
            Ok((
                vec![String::from("id"), String::from("name")],
                vec![
                    vec![String::from("1"), String::from("test1")],
                    vec![String::from("2"), String::from("test2")]
                ]
            ))
        );
        std::fs::remove_dir_all(&path);
    }
}
