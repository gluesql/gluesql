use {
    crate::{
        ast::Statement,
        executor::{execute, Payload},
        parse_sql::parse,
        plan::plan,
        result::Result,
        store::{GStore, GStoreMut},
        translate::translate,
    },
    futures::executor::block_on,
    std::{fmt::Debug, marker::PhantomData},
};

pub struct Glue<T: Debug, U: GStore<T> + GStoreMut<T>> {
    _marker: PhantomData<T>,
    pub storage: Option<U>,
}

impl<T: Debug, U: GStore<T> + GStoreMut<T>> Glue<T, U> {
    pub fn new(storage: U) -> Self {
        let storage = Some(storage);

        Self {
            _marker: PhantomData,
            storage,
        }
    }

    pub async fn plan(&self, sql: &str) -> Result<Statement> {
        let parsed = parse(sql)?;
        let statement = translate(&parsed[0])?;
        let storage = self.storage.as_ref().unwrap();

        plan(storage, statement).await
    }

    pub fn execute_stmt(&mut self, statement: Statement) -> Result<Payload> {
        block_on(self.execute_stmt_async(statement))
    }

    pub fn execute(&mut self, sql: &str) -> Result<Payload> {
        let statement = block_on(self.plan(sql))?;

        self.execute_stmt(statement)
    }

    pub async fn execute_stmt_async(&mut self, statement: Statement) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match execute(storage, &statement).await {
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

    pub async fn execute_async(&mut self, sql: &str) -> Result<Payload> {
        let statement = self.plan(sql).await?;

        self.execute_stmt_async(statement).await
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            executor::Payload,
            prelude::{Glue, Value},
            store::{GStore, GStoreMut},
        },
        std::fmt::Debug,
    };

    fn basic<T: Debug, U: GStore<T> + GStoreMut<T>>(mut glue: Glue<T, U>) {
        assert_eq!(
            glue.execute("DROP TABLE IF EXISTS api_test"),
            Ok(Payload::DropTable)
        );

        assert_eq!(
            glue.execute(
                "CREATE TABLE api_test (id INTEGER, name TEXT, nullable TEXT NULL, is BOOLEAN)"
            ),
            Ok(Payload::Create)
        );

        assert_eq!(
            glue.execute(
                "
                INSERT INTO
                    api_test (id, name, nullable, is)
                VALUES
                    (1, 'test1', 'not null', TRUE),
                    (2, 'test2', NULL, FALSE)"
            ),
            Ok(Payload::Insert(2))
        );

        assert_eq!(
            glue.execute("SELECT id, name, is FROM api_test"),
            Ok(Payload::Select {
                labels: vec![String::from("id"), String::from("name"), String::from("is")],
                rows: vec![
                    vec![
                        Value::I64(1),
                        Value::Str(String::from("test1")),
                        Value::Bool(true)
                    ],
                    vec![
                        Value::I64(2),
                        Value::Str(String::from("test2")),
                        Value::Bool(false)
                    ]
                ]
            })
        );
    }

    async fn basic_async<T: Debug, U: GStore<T> + GStoreMut<T>>(mut glue: Glue<T, U>) {
        assert_eq!(
            glue.execute_async("DROP TABLE IF EXISTS api_test").await,
            Ok(Payload::DropTable)
        );

        assert_eq!(
            glue.execute_async(
                "CREATE TABLE api_test (id INTEGER, name TEXT, nullable TEXT NULL, is BOOLEAN)"
            )
            .await,
            Ok(Payload::Create)
        );
    }

    #[cfg(feature = "sled-storage")]
    #[test]
    fn sled_basic() {
        use crate::storages::sled_storage::SledStorage;

        let config = sled::Config::default()
            .path("data/using_config")
            .temporary(true);

        let storage = SledStorage::try_from(config).unwrap();
        let glue = Glue::new(storage);

        basic(glue);
    }

    #[cfg(feature = "memory-storage")]
    #[test]
    fn memory_basic() {
        use crate::storages::memory_storage::MemoryStorage;

        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        basic(glue);
    }

    #[cfg(feature = "memory-storage")]
    #[test]
    fn memory_basic_async() {
        use futures::executor::block_on;

        use crate::storages::memory_storage::MemoryStorage;

        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        block_on(basic_async(glue));
    }
}
