//! Planning passes for [`StatementPlan`](crate::plan::StatementPlan).
//!
//! [`plan_schema_map`] prepares the statement's schemas before schema-dependent
//! validation and transformation. It returns the statement with a
//! [`Planned`](crate::plan::SchemaMapPlan::Planned) schema map; an already prepared
//! map is preserved, including an empty map. Callers can also supply schemas with
//! [`Build::with_schema`](crate::query_builder::Build::with_schema).
//!
//! [`validate`], [`validate_create_index`], [`validate_insert`],
//! [`plan_schemaless`], [`plan_primary_key`], [`plan_index`], [`plan_hash_join`], and
//! [`plan_create_table_columns`] read that map from the statement. They return
//! [`PlannerError::UnplannedSchemaMap`] if schema preparation has not run.
//! The transformation passes return `Result<StatementPlan>` and preserve the map.
//!
//! ```
//! use gluesql_core::{
//!     error::Result,
//!     plan::StatementPlan,
//!     planner::{plan_primary_key, plan_schema_map},
//!     store::Store,
//! };
//!
//! fn plan_lookup(storage: &impl Store, statement: StatementPlan) -> Result<StatementPlan> {
//!     let statement = plan_schema_map(storage, statement)?;
//!     plan_primary_key(statement)
//! }
//! ```
//!
//! [`Planner::plan`](crate::store::Planner::plan) orchestrates the complete planning
//! pipeline. The example above only performs schema preparation and primary-key
//! planning.

mod aggregate;
mod context;
mod create_table_columns;
mod custom_function;
mod error;
mod expr;
mod hash_join;
mod index;
mod primary_key;
mod query;
mod schema;
mod schemaless;
mod validate;
mod validate_create_index;
mod validate_insert;
mod visit;
mod visit_mut;

pub use {
    self::validate::validate,
    aggregate::plan as plan_aggregate,
    create_table_columns::plan as plan_create_table_columns,
    custom_function::{plan_custom_function_calls, validate_create_function},
    error::*,
    hash_join::plan as plan_hash_join,
    index::plan as plan_index,
    primary_key::plan as plan_primary_key,
    schema::plan_schema_map,
    schemaless::plan as plan_schemaless,
    validate_create_index::validate as validate_create_index,
    validate_insert::validate as validate_insert,
};
