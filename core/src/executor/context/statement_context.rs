use crate::plan::{CustomFunctionPlan, SchemaMap};

static EMPTY_SCHEMA_MAP: SchemaMap = SchemaMap::new();

#[derive(Clone, Copy)]
pub struct StatementContext<'a> {
    pub functions: &'a [CustomFunctionPlan],
    pub schema_map: &'a SchemaMap,
}

impl StatementContext<'_> {
    pub const EMPTY: Self = Self {
        functions: &[],
        schema_map: &EMPTY_SCHEMA_MAP,
    };
}
