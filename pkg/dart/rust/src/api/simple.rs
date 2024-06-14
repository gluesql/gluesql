pub use gluesql_core::{error::Error, prelude::Glue};
use {super::payload::DartPayload, flutter_rust_bridge::frb, memory_storage::MemoryStorage};

pub use {
    gluesql_core::{
        ast::DataType,
        chrono::{self, NaiveDate, NaiveDateTime, NaiveTime},
        data::Value,
        data::{Interval, Point},
        executor::Payload,
        executor::PayloadVariable,
    },
    rust_decimal::Decimal,
    std::net::IpAddr,
};

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
