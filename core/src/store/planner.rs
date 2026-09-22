use {
    super::{Store, statistics::UnknownStatistics},
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
    /// Produces a planned statement without changing its execution semantics.
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
    fn plan_with_statistics(&self, statement: StatementPlan) -> Result<PlannedStatement> {
        let plan = self.plan(statement)?;
        let unknown = UnknownStatistics;
        let provider = self.statistics_provider().unwrap_or(&unknown);
        let statistics = plan_statistics(provider, &plan)?;

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
            store::{RowIter, Statistic, Statistics, Store, TableStatistics},
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

        fn statistics_provider(&self) -> Option<&dyn Statistics> {
            Some(self)
        }
    }

    impl Planner for FailingStatisticsStorage {}

    impl Statistics for FailingStatisticsStorage {
        fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
            Err(Error::StorageMsg("statistics unavailable".to_owned()))
        }
    }

    /// Verifies provider errors are propagated through planning.
    #[test]
    fn propagates_statistics_provider_errors() {
        let storage = FailingStatisticsStorage(run("CREATE TABLE Foo (id INTEGER);"));
        let _ = storage.fetch_all_schemas();
        let _ = storage.fetch_data("Foo", &Key::None);
        let _ = storage.scan_data("Foo");
        let statement = translate(&parse("SELECT * FROM Foo").unwrap()[0])
            .unwrap()
            .into();

        assert_eq!(
            storage.plan_with_statistics(statement),
            Err(Error::StorageMsg("statistics unavailable".to_owned()))
        );
    }

    /// Verifies the default unknown-provider fallback estimate.
    #[test]
    fn plans_with_fallback_statistics_without_a_provider() {
        let storage = run("CREATE TABLE Foo (id INTEGER);");
        let statement = translate(&parse("SELECT * FROM Foo WHERE id = 1").unwrap()[0])
            .unwrap()
            .into();
        let planned = storage.plan_with_statistics(statement).unwrap();

        assert_eq!(
            planned.statistics.full_scans[0].cardinality,
            Statistic::Estimated(1_000)
        );
        assert_eq!(
            planned.statistics.filters[0].cardinality,
            Statistic::Estimated(100)
        );
    }
}
