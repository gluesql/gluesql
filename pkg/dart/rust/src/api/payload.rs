use flutter_rust_bridge::frb;
use gluesql_core::data::Value;
pub use gluesql_core::{
    ast::DataType,
    executor::{Payload, PayloadVariable},
};
use std::collections::HashMap;

#[frb(mirror(Payload), non_opaque)]
pub enum _Payload {
    ShowColumns(Vec<(String, DataType)>),
    Create,
    Insert(usize),
    Select {
        labels: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    SelectMap(Vec<HashMap<String, Value>>),
    Delete(usize),
    Update(usize),
    DropTable,
    DropFunction,
    AlterTable,
    CreateIndex,
    DropIndex,
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable(PayloadVariable),
}

// #[frb(non_opaque)]
// pub enum DartPayload {
//     ShowColumns(Vec<(String, DataType)>),
//     Create,
//     Insert(usize),
//     Select {
//         labels: Vec<String>,
//         rows: Vec<Vec<DartValue>>,
//     },
//     SelectMap(Vec<HashMap<String, DartValue>>),
//     Delete(usize),
//     Update(usize),
//     DropTable,
//     DropFunction,
//     AlterTable,
//     CreateIndex,
//     DropIndex,
//     StartTransaction,
//     Commit,
//     Rollback,
//     ShowVariable(PayloadVariable),
// }

// impl From<Payload> for DartPayload {
//     fn from(payload: Payload) -> Self {
//         match payload {
//             Payload::ShowColumns(columns) => DartPayload::ShowColumns(columns),
//             Payload::Create => DartPayload::Create,
//             Payload::Insert(affected) => DartPayload::Insert(affected),
//             Payload::Select { labels, rows } => {
//                 let rows = rows
//                     .into_iter()
//                     .map(|values| {
//                         values
//                             .into_iter()
//                             .map(|value| DartValue::from(value))
//                             .collect()
//                     })
//                     .collect();

//                 DartPayload::Select { labels, rows }
//             }
//             Payload::SelectMap(map) => {
//                 let map = map
//                     .into_iter()
//                     .map(|map| {
//                         map.into_iter()
//                             .map(|(key, value)| (key, DartValue::from(value)))
//                             .collect()
//                     })
//                     .collect();
//                 DartPayload::SelectMap(map)
//             }
//             Payload::Delete(affected) => DartPayload::Delete(affected),
//             Payload::Update(affected) => DartPayload::Update(affected),
//             Payload::DropTable => DartPayload::DropTable,
//             Payload::DropFunction => DartPayload::DropFunction,
//             Payload::AlterTable => DartPayload::AlterTable,
//             Payload::CreateIndex => DartPayload::CreateIndex,
//             Payload::DropIndex => DartPayload::DropIndex,
//             Payload::StartTransaction => DartPayload::StartTransaction,
//             Payload::Commit => DartPayload::Commit,
//             Payload::Rollback => DartPayload::Rollback,
//             Payload::ShowVariable(variable) => DartPayload::ShowVariable(variable),
//         }
//     }
// }
