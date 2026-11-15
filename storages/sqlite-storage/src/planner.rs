//! Runs the common planning passes, then delegates supported table filters to `SQLite`.

mod native_filter;

use {
    self::native_filter::plan as plan_native_filter,
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
        let statement = plan_primary_key(statement)?;
        let statement = plan_hash_join(statement)?;
        let statement = plan_aggregate(statement);

        validate_create_function(&statement)?;
        let statement = plan_custom_function_calls(self, statement)?;
        validate_insert(&statement)?;
        let statement = plan_create_table_columns(statement)?;

        // Resolve required scan columns before replacing a scan with a native source.
        let mut statement = plan_scan_columns(statement)?;

        if let StatementPlanKind::Query(query) = &mut statement.kind
            && let SchemaMapPlan::Planned(schemas) = &statement.schema_map
        {
            plan_native_filter(query, schemas);
        }

        Ok(statement)
    }
}
