use std::collections::HashMap;

// use super::error::DartError;
use flutter_rust_bridge::frb;
pub use gluesql_core::prelude::Glue;
use memory_storage::MemoryStorage;

pub use {
    gluesql_core::{
        ast::{
            Aggregate, AstLiteral, BinaryOperator, ColumnDef, DataType, DateTimeField, Expr,
            Function, Query, ToSql, UnaryOperator,
        },
        chrono::{self, NaiveDate, NaiveDateTime, NaiveTime, ParseError},
        data::{
            ConvertError, Interval, IntervalError, Key, KeyError, LiteralError,
            NumericBinaryOperator, Point, RowError, SchemaParseError, StringExtError, TableError,
            Value, ValueError,
        },
        error::{
            AggregateError, AlterError, AstBuilderError, Error, EvaluateError, ExecuteError,
            FetchError, InsertError, SelectError, SortError, TranslateError, UpdateError,
            ValidateError,
        },
        executor::Payload,
        executor::PayloadVariable,
        plan::PlanError,
        store::{AlterTableError, IndexError},
    },
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    std::net::IpAddr,
};

// #[frb(non_opaque, mirror(Payload))]
pub fn execute(sql: String) -> Result<Vec<Payload>, Error> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut glue = Glue::new(MemoryStorage::default());

        glue.execute(sql)
            .await
            .map(|payloads| payloads.into_iter().collect::<Vec<_>>())
    })
}

#[flutter_rust_bridge::frb(init)]
pub fn init_app() {
    // Default utilities - feel free to customize
    flutter_rust_bridge::setup_default_user_utils();
}
