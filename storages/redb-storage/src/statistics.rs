use {
    super::RedbStorage,
    gluesql_core::{
        error::Result,
        store::{Statistics, TableStatistics},
    },
};

impl Statistics for RedbStorage {
    /// Exposes Redb table metadata through the core statistics capability.
    fn fetch_table_statistics(&self, table_name: &str) -> Result<TableStatistics> {
        self.0
            .fetch_table_statistics(table_name)
            .map_err(Into::into)
    }
}
