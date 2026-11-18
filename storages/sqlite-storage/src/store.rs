mod row_iter;
mod schema;

use {
    self::row_iter::SqliteRowIter,
    crate::{
        Result, SqliteStorage, SqliteStorageError,
        codec::{decode, encode_key},
        schema::{columns, key_column, primary_key, quote, rowid_name},
    },
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result as GlueResult,
        store::{RowIter, ScanColumns, Store},
    },
};

impl Store for SqliteStorage {
    fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        self.read_schema(table_name).map_err(Into::into)
    }

    fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        self.read_all_schemas().map_err(Into::into)
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<Vec<Value>>> {
        let schema = self.require_schema(table_name)?;

        self.read_row(&schema, key).map_err(Into::into)
    }

    fn scan_data<'a>(
        &'a self,
        table_name: &str,
        requested: ScanColumns<'_>,
    ) -> GlueResult<RowIter<'a>> {
        let schema = self.require_schema(table_name)?;
        let rows = self.scan_rows(&schema, requested)?;

        if primary_key(&columns(&schema)).is_some() {
            let mut rows = rows.collect::<Result<Vec<_>>>()?;
            // TEXT-backed primary keys need logical Key order for scan results.
            rows.sort_by(|(left, _), (right, _)| left.cmp(right));

            return Ok(Box::new(rows.into_iter().map(Ok)));
        }

        Ok(Box::new(rows.map(|row| row.map_err(Into::into))))
    }
}

impl SqliteStorage {
    fn read_row(&self, schema: &Schema, key: &Key) -> Result<Option<Vec<Value>>> {
        let (key_name, key_type) = key_column(schema)?;
        let Some(value) = encode_key(key.clone(), key_type)? else {
            return Ok(None);
        };

        let columns = columns(schema);
        let projection = columns
            .iter()
            .map(|column| quote(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {projection} FROM {} WHERE {} = ?1",
            quote(&schema.table_name),
            quote(key_name)
        );
        let mut statement = self.connection.prepare(&sql)?;
        let mut rows = statement.query([value])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        columns
            .iter()
            .enumerate()
            .map(|(index, column)| decode(row.get_ref(index)?, &column.data_type))
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }

    fn scan_rows(&self, schema: &Schema, requested: ScanColumns<'_>) -> Result<SqliteRowIter<'_>> {
        let columns = columns(schema);
        let selected = match requested {
            ScanColumns::All => (0..columns.len()).collect::<Vec<_>>(),
            ScanColumns::Selected(requested) => {
                if schema.column_defs.is_none() {
                    return Err(SqliteStorageError::InvalidProjection(
                        schema.table_name.clone(),
                    ));
                }

                let mut previous = None;
                for (name, index) in requested {
                    if previous.is_some_and(|previous| previous >= *index)
                        || columns
                            .get(*index)
                            .is_none_or(|column| &column.name != name)
                    {
                        return Err(SqliteStorageError::InvalidProjection(
                            schema.table_name.clone(),
                        ));
                    }

                    previous = Some(*index);
                }

                requested.iter().map(|(_, index)| *index).collect()
            }
        };

        let (key_name, key_type) = key_column(schema)?;
        let mut projection = vec![quote(key_name)];
        projection.extend(selected.iter().map(|index| quote(&columns[*index].name)));

        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            projection.join(", "),
            quote(&schema.table_name),
            quote(rowid_name(schema)?)
        );
        let statement = self.connection.prepare(&sql)?;

        let selected_types = selected
            .into_iter()
            .map(|index| columns[index].data_type.clone())
            .collect();

        SqliteRowIter::new(statement, key_type.clone(), selected_types)
    }
}
