mod row_iter;

use {
    self::row_iter::SqliteRowIter,
    crate::{
        Result, SqliteStorage, SqliteStorageError,
        schema::{columns, is_reserved, primary_key, quote, rowid_name},
    },
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result as GlueResult,
        store::{RowIter, ScanColumns, Store},
    },
};

pub(super) struct StoredRow {
    pub(super) rowid: i64,
    pub(super) key: Key,
    pub(super) values: Vec<Value>,
}

impl Store for SqliteStorage {
    fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        self.read_schema(table_name).map_err(Into::into)
    }

    fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        self.read_all_schemas().map_err(Into::into)
    }

    fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<Vec<Value>>> {
        let schema = self.require_schema(table_name)?;

        Ok(self
            .read_rows(&schema, ScanColumns::All)?
            .into_iter()
            .find(|row| &row.key == key)
            .map(|row| row.values))
    }

    fn scan_data<'a>(
        &'a self,
        table_name: &str,
        requested: ScanColumns<'_>,
    ) -> GlueResult<RowIter<'a>> {
        let schema = self.require_schema(table_name)?;

        if primary_key(&columns(&schema)).is_some() {
            let mut rows = self.read_rows(&schema, requested)?;
            // TEXT-backed primary keys need logical Key order for scan results.
            rows.sort_by(|left, right| left.key.cmp(&right.key));

            return Ok(Box::new(
                rows.into_iter().map(|row| Ok((row.key, row.values))),
            ));
        }

        let rows = self.scan_rows(&schema, requested)?;

        Ok(Box::new(rows.map(|row| {
            row.map(|row| (row.key, row.values)).map_err(Into::into)
        })))
    }
}

impl SqliteStorage {
    fn read_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut statement = self.connection.prepare(
            "SELECT name
             FROM sqlite_schema
             WHERE type = 'table' AND name NOT GLOB 'sqlite_*'
             ORDER BY name",
        )?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        names
            .into_iter()
            .filter(|name| !is_reserved(name))
            .map(|name| self.require_schema(&name))
            .collect()
    }

    pub(super) fn read_rows(
        &self,
        schema: &Schema,
        requested: ScanColumns<'_>,
    ) -> Result<Vec<StoredRow>> {
        self.scan_rows(schema, requested)?.collect()
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

        let primary = primary_key(&columns);
        let mut projection = vec![quote(rowid_name(schema)?)];
        if let Some(index) = primary {
            projection.push(quote(&columns[index].name));
        }
        projection.extend(selected.iter().map(|index| quote(&columns[*index].name)));

        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            projection.join(", "),
            quote(&schema.table_name),
            projection[0]
        );
        let statement = self.connection.prepare(&sql)?;

        let primary_type = primary.map(|index| columns[index].data_type.clone());
        let selected_types = selected
            .into_iter()
            .map(|index| columns[index].data_type.clone())
            .collect();

        SqliteRowIter::new(statement, primary_type, selected_types)
    }
}
