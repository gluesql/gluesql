#![cfg(feature = "sled-storage")]
use {
    crate::{execute, storages::SledStorage, ExecuteError, Payload, Query, Result, Value},
    futures::executor::block_on,
    sqlparser::ast::{
        Expr, Ident, ObjectName, Query as AstQuery, SetExpr, Statement, Value as Literal, Values,
    },
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

    pub fn insert_vec(
        &mut self,
        table_name: String,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    ) -> Result<Payload> {
        // TODO: Make this more efficient and nicer by checking the way we execute
        let sqlparser_rows: Vec<Vec<Expr>> = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| {
                        Expr::Value(match cell {
                            Value::Null => Literal::Null,
                            Value::Bool(value) => Literal::Boolean(value),
                            Value::I64(value) => Literal::Number(value.to_string(), false),
                            Value::F64(value) => Literal::Number(value.to_string(), false),
                            Value::Str(value) => Literal::SingleQuotedString(value),
                        })
                    })
                    .collect()
            })
            .collect();
        let query = Query(Statement::Insert {
            table_name: ObjectName(vec![Ident {
                value: table_name,
                quote_style: None,
            }]),
            columns: columns
                .into_iter()
                .map(|name| Ident {
                    value: name,
                    quote_style: None,
                })
                .collect(),
            or: None,
            table: false,
            after_columns: vec![],
            overwrite: false,
            partitioned: None,
            source: Box::new(AstQuery {
                with: None,
                body: SetExpr::Values(Values(sqlparser_rows)),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            }),
        });
        let result = self.execute(&query);
        result
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

        assert_eq!(
            glue.execute(
                &parse_single("CREATE TABLE api_insert_vec (name TEXT, rating FLOAT)").unwrap()
            ),
            Ok(Payload::Create)
        );

        assert_eq!(
            glue.insert_vec(
                String::from("api_insert_vec"),
                vec![String::from("name"), String::from("rating")],
                vec![vec![Value::Str(String::from("test")), Value::F64(1.2)]]
            ),
            Ok(Payload::Insert(1))
        );

        assert_eq!(
            glue.execute(&parse_single("SELECT * FROM api_insert_vec").unwrap()),
            Ok(Payload::Select {
                labels: vec![String::from("name"), String::from("rating")],
                rows: vec![Row(vec![Value::Str(String::from("test")), Value::F64(1.2)])]
            })
        );

        assert_eq!(
            glue.insert_vec(
                String::from("api_insert_vec"),
                vec![String::from("name"), String::from("rating")],
                vec![
                    vec![Value::Str(String::from("test2")), Value::F64(1.3)],
                    vec![Value::Str(String::from("test3")), Value::F64(1.0)],
                    vec![Value::Str(String::from("test4")), Value::F64(100000.94)]
                ]
            ),
            Ok(Payload::Insert(3))
        );

        assert_eq!(
            glue.execute(&parse_single("SELECT * FROM api_insert_vec").unwrap()),
            Ok(Payload::Select {
                labels: vec![String::from("name"), String::from("rating")],
                rows: vec![
                    Row(vec![Value::Str(String::from("test")), Value::F64(1.2)]),
                    Row(vec![Value::Str(String::from("test2")), Value::F64(1.3)]),
                    Row(vec![Value::Str(String::from("test3")), Value::F64(1.0)]),
                    Row(vec![
                        Value::Str(String::from("test4")),
                        Value::F64(100000.94)
                    ])
                ]
            })
        );
    }
}
