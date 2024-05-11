use super::payload::DartPayload;
use flutter_rust_bridge::frb;
use gluesql_core::prelude::Glue;
pub use gluesql_core::{data::Value, error::Error, executor::Payload};
use memory_storage::MemoryStorage;

#[frb(sync)] // Asynchronous mode for simplicity of the demo
pub fn execute(sql: String) -> Result<Vec<DartPayload>, Error> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut glue = Glue::new(MemoryStorage::default());

        glue.execute(sql).await.map(|payloads| {
            payloads
                .into_iter()
                .map(|x| DartPayload::from(x))
                .collect::<Vec<_>>()
        })
    })
}

#[flutter_rust_bridge::frb(init)]
pub fn init_app() {
    // Default utilities - feel free to customize
    flutter_rust_bridge::setup_default_user_utils();
}
