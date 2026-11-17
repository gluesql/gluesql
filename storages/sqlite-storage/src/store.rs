mod row_iter;
mod schema;

use {
    self::row_iter::SqliteRowIter,
    crate::{
        Result, SqliteStorage, SqliteStorageError,
        codec::{decode, encode},
        schema::{columns, primary_key, quote, rowid_name},
    },
    gluesql_core::{
        ast::DataType,
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

        self.read_row(&schema, key).map_err(Into::into)
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
    fn read_row(&self, schema: &Schema, key: &Key) -> Result<Option<Vec<Value>>> {
        let columns = columns(schema);
        let (key_column, key_type) = match primary_key(&columns) {
            Some(index) => (columns[index].name.as_str(), &columns[index].data_type),
            None => (rowid_name(schema)?, &DataType::Int),
        };
        let value = Value::from(key.clone());

        // Key equality does not coerce types, and SQLite cannot store NaN.
        if value.get_type().as_ref() != Some(key_type)
            || matches!(value, Value::F32(v) if v.is_nan())
            || matches!(value, Value::F64(v) if v.is_nan())
        {
            return Ok(None);
        }

        // Decimal keys such as 1.0 and 1.00 compare equal despite different TEXT encodings.
        if *key_type == DataType::Decimal {
            return Ok(self
                .read_rows(schema, ScanColumns::All)?
                .into_iter()
                .find(|row| &row.key == key)
                .map(|row| row.values));
        }

        let projection = columns
            .iter()
            .map(|column| quote(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {projection} FROM {} WHERE {} = ?1",
            quote(&schema.table_name),
            quote(key_column)
        );
        let mut statement = self.connection.prepare(&sql)?;
        let mut rows = statement.query([encode(&value, key_type)?])?;
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
