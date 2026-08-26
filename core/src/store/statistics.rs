use {
    crate::{data::Value, result::Result},
    serde::{Deserialize, Serialize},
};

/// A statistic together with the confidence the provider has in its value.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum Statistic<T> {
    /// The value is exact for the represented data.
    Exact(T),
    /// The value is an estimate for the represented data.
    Estimated(T),
    /// The provider does not have this statistic.
    #[default]
    Unknown,
}

/// Statistics describing a table as a whole.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Number of rows in the table.
    pub row_count: Statistic<u64>,
    /// Storage size of the table in bytes.
    pub size_bytes: Statistic<u64>,
}

/// Statistics describing a single table column.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Fraction of rows whose value is NULL, in the range from 0.0 to 1.0.
    pub null_ratio: Statistic<f64>,
    /// Number of distinct non-NULL values in the column.
    pub distinct_count: Statistic<u64>,
    /// Minimum non-NULL value in the column.
    pub min_value: Statistic<Value>,
    /// Maximum non-NULL value in the column.
    pub max_value: Statistic<Value>,
}

/// Optional statistics supplied by a storage implementation.
pub trait Statistics {
    /// Fetch statistics for a table.
    ///
    /// Storage implementations that do not maintain table statistics can use
    /// this default implementation. It returns an explicit unknown value and
    /// does not scan the table as a fallback.
    fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
        Ok(TableStatistics::default())
    }

    /// Fetch statistics for a column in a table.
    ///
    /// Storage implementations that do not maintain column statistics can use
    /// this default implementation. It returns an explicit unknown value and
    /// does not scan the table as a fallback.
    fn fetch_column_statistics(
        &self,
        _table_name: &str,
        _column_name: &str,
    ) -> Result<ColumnStatistics> {
        Ok(ColumnStatistics::default())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{ColumnStatistics, Statistic, Statistics, TableStatistics},
        crate::{
            data::Value,
            result::{Error, Result},
        },
    };

    struct UnknownStatistics;

    impl Statistics for UnknownStatistics {}

    struct ProvidedStatistics;

    impl Statistics for ProvidedStatistics {
        fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
            Ok(TableStatistics {
                row_count: Statistic::Exact(3),
                size_bytes: Statistic::Estimated(128),
            })
        }

        fn fetch_column_statistics(
            &self,
            _table_name: &str,
            _column_name: &str,
        ) -> Result<ColumnStatistics> {
            Ok(ColumnStatistics {
                null_ratio: Statistic::Exact(0.25),
                distinct_count: Statistic::Estimated(2),
                min_value: Statistic::Exact(Value::I32(1)),
                max_value: Statistic::Exact(Value::I32(4)),
            })
        }
    }

    struct FailingStatistics;

    impl Statistics for FailingStatistics {
        fn fetch_table_statistics(&self, _table_name: &str) -> Result<TableStatistics> {
            Err(Error::StorageMsg("statistics unavailable".to_owned()))
        }
    }

    #[test]
    fn default_statistics_are_unknown() {
        let storage = UnknownStatistics;

        assert_eq!(
            storage.fetch_table_statistics("users").unwrap(),
            TableStatistics::default()
        );
        assert_eq!(
            storage.fetch_column_statistics("users", "name").unwrap(),
            ColumnStatistics::default()
        );
    }

    #[test]
    fn statistics_preserve_accuracy_and_values() {
        let storage = ProvidedStatistics;

        assert_eq!(
            storage.fetch_table_statistics("users").unwrap(),
            TableStatistics {
                row_count: Statistic::Exact(3),
                size_bytes: Statistic::Estimated(128),
            }
        );
        assert_eq!(
            storage.fetch_column_statistics("users", "age").unwrap(),
            ColumnStatistics {
                null_ratio: Statistic::Exact(0.25),
                distinct_count: Statistic::Estimated(2),
                min_value: Statistic::Exact(Value::I32(1)),
                max_value: Statistic::Exact(Value::I32(4)),
            }
        );
    }

    #[test]
    fn statistics_errors_are_propagated() {
        let error = FailingStatistics
            .fetch_table_statistics("users")
            .expect_err("storage error should be returned");

        assert_eq!(
            error,
            Error::StorageMsg("statistics unavailable".to_owned())
        );
    }
}
