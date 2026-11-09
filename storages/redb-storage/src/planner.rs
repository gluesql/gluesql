use {
    crate::RedbStorage,
    gluesql_core::{
        error::Result,
        plan::StatementPlan,
        planner::{
            plan_aggregate, plan_create_table_columns, plan_custom_function_calls, plan_hash_join,
            plan_index, plan_primary_key, plan_schema_map, plan_schemaless, validate,
            validate_create_function, validate_create_index, validate_insert,
        },
        store::Planner,
    },
};

impl Planner for RedbStorage {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let statement = plan_schema_map(self, statement)?;

        validate(&statement)?;
        validate_create_index(&statement)?;

        let statement = plan_schemaless(statement)?;
        let statement = plan_primary_key(statement)?;
        let statement = plan_index(statement)?;
        let statement = plan_hash_join(statement)?;
        let statement = plan_aggregate(statement);

        validate_create_function(&statement)?;
        let statement = plan_custom_function_calls(self, statement)?;

        validate_insert(&statement)?;
        plan_create_table_columns(statement)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{RedbStorage, migration::initialize_storage_format_version},
        gluesql_core::{
            data::Schema,
            query_builder::{Build, non_clustered, table},
            store::Planner,
        },
        redb::{Database, backends::InMemoryBackend},
    };

    #[test]
    fn plan() {
        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        initialize_storage_format_version(&db).unwrap();
        let storage = RedbStorage::from_database(db).unwrap();
        let schema = Schema::from_ddl(
            "
            CREATE TABLE Item (id INT);
            CREATE INDEX by_id ON Item(id);
        ",
        )
        .unwrap();
        let input = table("Item")
            .select()
            .filter("id = 7")
            .with_schema(schema.clone())
            .with_schema("CREATE TABLE Unused (name TEXT)")
            .build()
            .unwrap();
        let actual = storage.plan(input);
        let expected = table("Item")
            .index_by(non_clustered("by_id".to_owned()).eq("7"))
            .select()
            .with_schema(schema)
            .with_schema("CREATE TABLE Unused (name TEXT)")
            .build();
        assert_eq!(actual, expected);
    }
}
