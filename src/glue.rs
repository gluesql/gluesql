#![cfg(feature = "sled-storage")]
use {
    crate::{
        ast::Statement, execute, parse, plan, storages::SledStorage, translate, Payload, Result,
    },
    futures::executor::block_on,
};

pub struct Glue {
    pub storage: Option<SledStorage>,
}

impl Glue {
    pub fn new(storage: SledStorage) -> Self {
        let storage = Some(storage);

        Self { storage }
    }

    pub fn plan(&self, sql: &str) -> Result<Statement> {
        let parsed = parse(sql)?;
        let statement = translate(&parsed[0])?;
        let storage = self.storage.as_ref().unwrap();

        block_on(plan(storage, statement))
    }

    pub fn execute_stmt(&mut self, statement: Statement) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match block_on(execute(storage, &statement)) {
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

    pub fn execute(&mut self, sql: &str) -> Result<Payload> {
        let statement = self.plan(sql)?;

        self.execute_stmt(statement)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{Glue, Payload, Row, SledStorage, Value},
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
            glue.execute("DROP TABLE IF EXISTS api_test"),
            Ok(Payload::DropTable)
        );
        assert_eq!(
            glue.execute("CREATE TABLE api_test (id INTEGER PRIMARY KEY, name TEXT, nullable TEXT NULL, is BOOLEAN)"),
            Ok(Payload::Create)
        );
        assert_eq!(
            glue.execute("INSERT INTO api_test (id, name, nullable, is) VALUES (1, 'test1', 'not null', TRUE), (2, 'test2', NULL, FALSE)"),
            Ok(Payload::Insert(2))
        );

        assert_eq!(
            glue.execute("SELECT id, name, is FROM api_test"),
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
    }
}
