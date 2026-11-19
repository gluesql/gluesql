mod row_iter;

use {
    self::row_iter::SqliteNativeRowIter,
    crate::{SqliteStorage, SqliteStorageError, codec::encode},
    gluesql_core::{
        ast::DataType,
        data::Value,
        error::Result,
        store::{NativeQuery, NativeRowIter},
    },
    rusqlite::types::Value as SqlValue,
};

impl NativeQuery for SqliteStorage {
    fn query_native<'a>(
        &'a self,
        query: &str,
        params: &[Value],
        output_types: &[DataType],
    ) -> Result<NativeRowIter<'a>> {
        let statement = self
            .connection
            .prepare(query)
            .map_err(SqliteStorageError::from)?;
        if !statement.readonly() {
            return Err(SqliteStorageError::NativeStatementNotReadOnly.into());
        }

        // An empty projection uses SELECT 1 to preserve rows without exposing a column.
        let expected = output_types.len().max(1);
        let actual = statement.column_count();
        if actual != expected {
            return Err(SqliteStorageError::NativeColumnCountMismatch { expected, actual }.into());
        }

        let params = params
            .iter()
            .map(|value| match value.get_type() {
                Some(data_type) => encode(value, &data_type),
                None => Ok(SqlValue::Null),
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let rows = SqliteNativeRowIter::new(statement, &params, output_types.to_vec())?;

        Ok(Box::new(rows.map(|row| row.map_err(Into::into))))
    }
}
