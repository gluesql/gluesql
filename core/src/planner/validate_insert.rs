use {
    super::PlannerError,
    crate::{
        plan::{SchemaMapPlan, StatementPlan, StatementPlanKind},
        result::Result,
    },
};

pub fn validate(statement: &StatementPlan) -> Result<()> {
    let SchemaMapPlan::Planned(schema_map) = &statement.schema_map else {
        return Err(PlannerError::UnplannedSchemaMap.into());
    };

    if let StatementPlanKind::Insert { table_name, .. } = &statement.kind
        && !schema_map.contains_key(table_name)
    {
        return Err(PlannerError::InsertTableNotFound(table_name.clone()).into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        plan::{SchemaMap, SchemaMapPlan},
        planner::PlannerError,
        query_builder::{Build, table},
        result::Error,
    };

    #[test]
    fn validate() {
        let mut input = table("Item").insert().values(vec!["1"]).build().unwrap();
        let actual = super::validate(&input);
        let expected = Err(Error::Planner(PlannerError::UnplannedSchemaMap));
        assert_eq!(actual, expected);

        input.schema_map = SchemaMapPlan::Planned(SchemaMap::new());
        let actual = super::validate(&input);
        let expected = Err(Error::Planner(PlannerError::InsertTableNotFound(
            "Item".to_owned(),
        )));
        assert_eq!(actual, expected);

        for ddl in ["CREATE TABLE Item (id INT)", "CREATE TABLE Item"] {
            let input = table("Item")
                .insert()
                .values(vec!["1"])
                .with_schema(ddl)
                .build()
                .unwrap();
            let actual = super::validate(&input);
            let expected = Ok(());
            assert_eq!(actual, expected, "{ddl}");
        }

        let mut input = table("Item").select().build().unwrap();
        input.schema_map = SchemaMapPlan::Planned(SchemaMap::new());
        let actual = super::validate(&input);
        let expected = Ok(());
        assert_eq!(actual, expected);
    }
}
