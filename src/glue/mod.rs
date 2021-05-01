#![cfg(feature = "sled-storage")]
use {
    crate::{execute, storages::SledStorage, ExecuteError, Payload, Query, Result},
    futures::executor::block_on,
};

mod value;

pub struct Glue {
    storage: Option<SledStorage>,
}

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

    pub fn select_as_string(&mut self, query: &Query) -> Result<Vec<Vec<String>>> {
        // TODO: Make this more efficient and not affect database if not select by converting earlier
        if let Ok(Payload::Select { labels, rows }) = self.execute(query) {
            Ok(vec![labels] // Gross
                .into_iter()
                .chain(
                    rows.into_iter()
                        .map(|row| {
                            row.0
                                .into_iter()
                                .map(|value| (&value).into())
                                .collect::<Vec<String>>()
                        })
                        .collect::<Vec<Vec<String>>>(),
                )
                .collect())
        } else {
            Err(ExecuteError::QueryNotSupported.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{parse_sql::parse_single, Glue, Payload, Row, SledStorage, Value},
        std::convert::TryFrom,
    };

    #[test]
    fn eq() {
        let config = sled::Config::default()
            .path("data/using_config")
            .temporary(true);

        let sled = SledStorage::try_from(config).unwrap();
        let mut glue = Glue::new(sled);
        assert_eq!(
            glue.execute(
                &parse_single(
                    "CREATE TABLE api_test (id INTEGER PRIMARY KEY, name TEXT, nullable TEXT NULL, is BOOLEAN)"
                )
                .unwrap(),
            ),
            Ok(Payload::Create)
        );
        assert_eq!(
            glue.execute(
                &parse_single("INSERT INTO api_test (id, name, nullable, is) VALUES (1, 'test1', 'not null', TRUE), (2, 'test2', NULL, FALSE)")
                    .unwrap()
            ),
            Ok(Payload::Insert(2))
        );

        assert_eq!(
            glue.execute(&parse_single("SELECT id, name, is FROM api_test").unwrap()), // Not selecting NULL because NULL != NULL. TODO: Expand this test so that NULL == NULL
            Ok(Payload::Select {
                labels: vec![String::from("id"), String::from("name"), String::from("is")],
                rows: vec![
                    Row(vec![
                        Value::I64(1),
                        Value::Str(String::from("test1")),
                        Value::Bool(true)
                    ]),
                    Row(vec![
                        Value::I64(2),
                        Value::Str(String::from("test2")),
                        Value::Bool(false)
                    ])
                ]
            })
        );
        assert_eq!(
            glue.select_as_string(&parse_single("SELECT * FROM api_test").unwrap()),
            Ok(vec![
                vec![
                    String::from("id"),
                    String::from("name"),
                    String::from("nullable"),
                    String::from("is")
                ],
                vec![
                    String::from("1"),
                    String::from("test1"),
                    String::from("not null"),
                    String::from("TRUE")
                ],
                vec![
                    String::from("2"),
                    String::from("test2"),
                    String::from("NULL"),
                    String::from("FALSE")
                ]
            ])
        );

        use std::convert::TryInto;

        let test_value: String = Value::Str(String::from("test")).into();
        assert_eq!(test_value, String::from("test"));
        let test_value: String = (&Value::Str(String::from("test"))).into();
        assert_eq!(test_value, String::from("test"));
        let test_value: Result<String, _> = Value::I64(1).try_into();
        assert_eq!(test_value, Ok(String::from("1")));
        let test_value: Result<String, _> = (&Value::I64(1)).try_into();
        assert_eq!(test_value, Ok(String::from("1")));
    }
}
