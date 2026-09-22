use {
    super::{Statistics, Store},
    crate::{
        plan::StatementPlan,
        planner::{
            PlannedStatement, fetch_schema_map, plan_aggregate, plan_hash_join, plan_primary_key,
            plan_schemaless, plan_statistics, validate,
        },
        result::Result,
    },
};

pub trait Planner: Store {
    fn plan(&self, statement: StatementPlan) -> Result<StatementPlan> {
        let schema_map = fetch_schema_map(self, &statement)?;
        validate(&schema_map, &statement)?;

        let statement = plan_schemaless(&schema_map, statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_hash_join(&schema_map, statement);
        let statement = plan_aggregate(statement);

        Ok(statement)
    }

    /// Plans a statement and returns estimates without changing its plan.
    fn plan_with_statistics(&self, statement: StatementPlan) -> Result<PlannedStatement>
    where
        Self: Statistics,
    {
        let plan = self.plan(statement)?;
        let statistics = plan_statistics(self, &plan)?;

        Ok(PlannedStatement { plan, statistics })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Planner,
        crate::{
            data::{Key, Schema, Value},
            mock::{MockStorage, run},
            parse_sql::parse,
            result::{Error, Result},
            store::{RowIter, Statistics, Store, TableStatistics},
            translate::translate,
        },
    };

    struct FailingStatisticsStorage(MockStorage);

    impl Store for FailingStatisticsStorage {
        fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
            self.0.fetch_schema(table_name)
        }

        fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
            self.0.fetch_all_schemas()
        }

        fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
            self.0.fetch_data(table_name, key)
        }

        fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
            self.0.scan_data(table_name)
        }
    }

    impl Planner for FailingStatisticsStorage {}

    impl Statistics for FailingStatisticsStorage {
        fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
            Err(Error::StorageMsg("statistics unavailable".to_owned()))
        }
    }

    #[test]
    fn propagates_statistics_provider_errors() {
        let storage = FailingStatisticsStorage(run("CREATE TABLE Foo (id INTEGER);"));
        let statement = translate(&parse("SELECT * FROM Foo").unwrap()[0])
            .unwrap()
            .into();

        assert_eq!(
            storage.plan_with_statistics(statement),
            Err(Error::StorageMsg("statistics unavailable".to_owned()))
        );
    }
}
