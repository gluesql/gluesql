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
    std::marker::PhantomData,
};

pub struct Glue<T, U: GStore<T> + GStoreMut<T>> {
    _marker: PhantomData<T>,
    pub storage: Option<U>,
}

impl<T, U: GStore<T> + GStoreMut<T>> Glue<T, U> {
    pub fn new(storage: U) -> Self {
        let storage = Some(storage);

        Self {
            _marker: PhantomData,
            storage,
        }
    }

    pub async fn plan<Sql: AsRef<str>>(&self, sql: Sql) -> Result<Vec<Statement>> {
        let parsed = parse(sql)?;
        let storage = self.storage.as_ref().unwrap();

        let mut statements = Vec::<Statement>::new();
        let parsed_iter = parsed.iter();
        for parsed in parsed_iter {
            let translated = translate(parsed)?;
            let planned = plan(storage, translated).await?;
            statements.push(planned);
        }
        Ok(statements)
    }

    pub fn execute_stmt(&mut self, statement: &Statement) -> Result<Payload> {
        block_on(self.execute_stmt_async(statement))
    }

    pub fn execute<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        let statements = block_on(self.plan(sql))?;

        let mut payloads = Vec::<Payload>::new();
        for statement in statements.iter() {
            let payload = self.execute_stmt(statement)?;
            payloads.push(payload);
        }
        Ok(payloads)
    }

    pub async fn execute_stmt_async(&mut self, statement: &Statement) -> Result<Payload> {
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

    pub async fn execute_async<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {
        let statements = self.plan(sql).await?;

        let mut payloads = Vec::<Payload>::new();
        for statement in statements.iter() {
            let payload = self.execute_stmt_async(statement).await?;
            payloads.push(payload);
        }
        Ok(payloads)
    }
}
