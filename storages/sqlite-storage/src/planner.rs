//! Delegates filters before JOIN planning and remaining table scans after it.

mod native_filter;
mod native_scan;
mod table_read;

use {
    self::{native_filter::plan as plan_native_filter, native_scan::plan as plan_native_scan},
    crate::SqliteStorage,
    gluesql_core::{
        error::Result,
        plan::{SchemaMapPlan, StatementPlan, StatementPlanKind},
        planner::{
            plan_aggregate, plan_create_table_columns, plan_custom_function_calls, plan_hash_join,
            plan_primary_key, plan_scan_columns, plan_schema_map, plan_schemaless, validate,
            validate_create_function, validate_insert,
        },
        store::Planner,
    },
};

impl Planner for SqliteStorage {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        // Native translation depends on the common planner's normalization and validation.
        let statement = plan_schema_map(self, statement)?;
        validate(&statement)?;

        let statement = plan_schemaless(statement)?;

        // Resolve required scan columns before replacing a scan with a native source.
        let statement = plan_scan_columns(statement)?;
        let mut statement = plan_primary_key(statement)?;

        if let StatementPlanKind::Query(query) = &mut statement.kind
            && let SchemaMapPlan::Planned(schemas) = &statement.schema_map
        {
            plan_native_filter(query, schemas);
        }

        let mut statement = plan_hash_join(statement)?;
        if let StatementPlanKind::Query(query) = &mut statement.kind
            && let SchemaMapPlan::Planned(schemas) = &statement.schema_map
        {
            plan_native_scan(query, schemas);
        }

        let statement = plan_aggregate(statement);

        validate_create_function(&statement)?;
        let statement = plan_custom_function_calls(self, statement)?;
        validate_insert(&statement)?;
        plan_create_table_columns(statement)
    }
}
