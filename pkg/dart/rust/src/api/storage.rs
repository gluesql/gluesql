// use ::memory_storage::MemoryStorage;
// use gluesql_core::executor::execute;

// pub enum DartStorage {
//     MemoryStorage(MemoryStorage),
// }

// impl DartStorage {
//     pub fn execute(&self, sql: String) -> String {
//         todo!()
//         // match self {
//         //     DartStorage::MemoryStorage(storage) => execute(&mut *storage, &statement).await,
//         // }
//     }
// }

// #[tokio::main]
// pub async fn plan_query(storage: &ExStorage, statement: Statement) -> ExResult<Statement> {
//     match storage {
//         ExStorage::MemoryStorage(storage) => {
//             let storage = storage.resource.locked_storage.read().await;

//             plan(&*storage, statement).await.map_err(|e| e.to_string())
//         }
//     }
// }

// #[tokio::main]
// pub async fn execute_query(storage: &mut ExStorage, statement: Statement) -> ExResult<Payload> {
//     match storage {
//         ExStorage::MemoryStorage(storage) => {
//             let mut storage = storage.resource.locked_storage.write().await;

//             execute(&mut *storage, &statement)
//                 .await
//                 .map_err(|e| e.to_string())
//         }
//     }
// }
