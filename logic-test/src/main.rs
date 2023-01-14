use sqllogictest::ColumnType;

use {
    gluesql_core::prelude::{Glue, Payload},
    sqllogictest::DBOutput,
};

pub struct MemoryStorage(Glue<gluesql_memory_storage::MemoryStorage>);

impl MemoryStorage {
    pub fn new() -> Self {
        let storage = gluesql_memory_storage::MemoryStorage::default();
        let glue = Glue::new(storage);

        Self(glue)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SqlLogicTestError {
    #[error("failed execute")]
    FailedExecute,
}

impl sqllogictest::DB for MemoryStorage {
    type Error = SqlLogicTestError;

    fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        let mut result = self.0.execute(sql).map_err(|err| {
            eprintln!("{err}");

            SqlLogicTestError::FailedExecute
        })?;

        assert_eq!(result.len(), 1);
        let result = result.pop().unwrap();

        match result {
            Payload::Insert(count) | Payload::Delete(count) => {
                Ok(DBOutput::StatementComplete(count.try_into().unwrap()))
            }
            Payload::Select { labels, rows } => Ok(DBOutput::Rows {
                types: labels.into_iter().map(|_| ColumnType::Any).collect(),
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|value| value.into()).collect())
                    .collect(),
            }),

            Payload::ShowColumns(_) => todo!(),
            Payload::SelectMap(_) => todo!(),
            Payload::Update(_) => todo!(),
            Payload::Rollback => todo!(),
            Payload::ShowVariable(_) => todo!(),

            Payload::Create
            | Payload::DropTable
            | Payload::AlterTable
            | Payload::CreateIndex
            | Payload::DropIndex
            | Payload::StartTransaction
            | Payload::Commit => Ok(DBOutput::StatementComplete(1)),
        }
    }

    fn engine_name(&self) -> &str {
        "gluesql-memory-stroage"
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 12)]
async fn main() {
    let storage = MemoryStorage::new();

    let mut tester = sqllogictest::Runner::new(storage);

    let current_dir = std::env::current_dir().unwrap();
    let script =
        std::fs::read_to_string(current_dir.join("./logic-test/slt/select/select1.test")).unwrap();
    let records = sqllogictest::parse(&script).unwrap();
    for record in records {
        println!("{record}");
        tester.run_async(record).await.unwrap();
    }
}
