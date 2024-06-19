use std::collections::HashMap;

// use super::error::DartError;
use flutter_rust_bridge::frb;
pub use gluesql_core::prelude::Glue;
use gluesql_core::{
    ast::Statement,
    store::{GStore, GStoreMut},
};
use gluesql_memory_storage::MemoryStorage;

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
        prelude::Result,
        store::{AlterTableError, IndexError},
    },
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    std::net::IpAddr,
};

#[frb(non_opaque)]
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

// #[frb(external)]
// impl<T: GStore + GStoreMut> Glue<T> {
//     pub fn new(storage: T) -> Self {}
//     pub async fn plan<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Statement>> {}
//     pub async fn execute_stmt(&mut self, statement: &Statement) -> Result<Payload> {}
//     pub async fn execute<Sql: AsRef<str>>(&mut self, sql: Sql) -> Result<Vec<Payload>> {}
// }
