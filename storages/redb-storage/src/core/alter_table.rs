use {
    super::{
        Result, SCHEMA_TABLE, StorageCore,
        index::{IndexTableDefinition, index_table_name},
    },
    crate::error::StorageError,
    bincode::{deserialize, serialize},
    gluesql_core::{
        ast::{ColumnDef, Expr, Function},
        data::{Key, Value},
        store::AlterTableError,
    },
    redb::ReadableTable,
};

impl StorageCore {
    pub(crate) fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        let old_def = Self::data_table_def(table_name)?;
        let new_def = Self::data_table_def(new_table_name)?;
        let txn = self.txn()?;
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        new_table_name.clone_into(&mut schema.table_name);
        let payload = serialize(&schema)?;

        txn.rename_table(old_def, new_def)?;
        for index in &schema.indexes {
            let old_name = index_table_name(table_name, &index.name);
            let new_name = index_table_name(new_table_name, &index.name);
            txn.rename_multimap_table(
                IndexTableDefinition::new(&old_name),
                IndexTableDefinition::new(&new_name),
            )?;
        }
        let mut schemas = txn.open_table(SCHEMA_TABLE)?;
        schemas.remove(table_name)?;
        schemas.insert(new_table_name, payload)?;
        Ok(())
    }

    pub(crate) fn rename_column(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        self.txn()?;
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let columns = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;
        if columns.iter().any(|column| column.name == new_name) {
            return Err(AlterTableError::AlreadyExistingColumn(new_name.to_owned()).into());
        }
        let column = columns
            .iter_mut()
            .find(|column| column.name == old_name)
            .ok_or(AlterTableError::RenamingColumnNotFound)?;
        new_name.clone_into(&mut column.name);
        for index in &mut schema.indexes {
            rename_index_column(&mut index.expr, old_name, new_name)?;
        }
        self.insert_schema(&schema)
    }

    pub(crate) fn add_column(
        &mut self,
        table_name: &str,
        column: &ColumnDef,
        default: &Value,
    ) -> Result<()> {
        let txn = self.txn()?;
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let columns = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;
        if columns.iter().any(|existing| existing.name == column.name) {
            return Err(AlterTableError::AlreadyExistingColumn(column.name.clone()).into());
        }
        columns.push(column.clone());
        let schema_payload = serialize(&schema)?;
        let mut data = txn.open_table(Self::data_table_def(table_name)?)?;
        let rows = data
            .iter()?
            .map(|entry| {
                let (key, payload) = entry?;
                let (row_key, mut row): (Key, Vec<Value>) = deserialize(&payload.value())?;
                row.push(default.clone());
                Ok((key.value().to_vec(), serialize(&(row_key, row))?))
            })
            .collect::<Result<Vec<_>>>()?;
        for (key, payload) in rows {
            data.insert(key.as_slice(), payload)?;
        }
        txn.open_table(SCHEMA_TABLE)?
            .insert(table_name, schema_payload)?;
        Ok(())
    }

    pub(crate) fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let txn = self.txn()?;
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let columns = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;
        let position = match columns.iter().position(|column| column.name == column_name) {
            Some(position) => position,
            None if if_exists => return Ok(()),
            None => {
                return Err(AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into());
            }
        };
        columns.remove(position);
        let schema_payload = serialize(&schema)?;
        let mut data = txn.open_table(Self::data_table_def(table_name)?)?;
        let rows = data
            .iter()?
            .map(|entry| {
                let (key, payload) = entry?;
                let (row_key, mut row): (Key, Vec<Value>) = deserialize(&payload.value())?;
                if position >= row.len() {
                    return Err(StorageError::ConflictDroppingColumnIndex(position));
                }
                row.remove(position);
                Ok((key.value().to_vec(), serialize(&(row_key, row))?))
            })
            .collect::<Result<Vec<_>>>()?;
        for (key, payload) in rows {
            data.insert(key.as_slice(), payload)?;
        }
        txn.open_table(SCHEMA_TABLE)?
            .insert(table_name, schema_payload)?;
        Ok(())
    }
}

fn rename_index_column(expr: &mut Expr, old_name: &str, new_name: &str) -> Result<()> {
    match expr {
        Expr::Identifier(name) => {
            if name == old_name {
                new_name.clone_into(name);
            }
        }
        Expr::Nested(expr) | Expr::UnaryOp { expr, .. } => {
            rename_index_column(expr, old_name, new_name)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            rename_index_column(left, old_name, new_name)?;
            rename_index_column(right, old_name, new_name)?;
        }
        Expr::Function(function) => match function.as_mut() {
            Function::Cast { expr, .. } => rename_index_column(expr, old_name, new_name)?,
            _ => return Err(StorageError::ConflictRenamingIndexExpression),
        },
        Expr::Literal(_) | Expr::TypedString { .. } => (),
        _ => return Err(StorageError::ConflictRenamingIndexExpression),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::rename_index_column,
        crate::error::StorageError,
        gluesql_core::{ast::Expr, parse_sql::parse_expr, translate::translate_expr},
    };

    fn expr(sql: &str) -> Expr {
        translate_expr(&parse_expr(sql).expect(sql), &[]).expect(sql)
    }

    #[test]
    fn rename_index_references() {
        let test = |input, expected| {
            let mut actual = expr(input);
            rename_index_column(&mut actual, "n", "renamed").unwrap();
            assert_eq!(actual, expr(expected), "{input}");
        };
        test("n", "renamed");
        test("other", "other");
        test("(n)", "(renamed)");
        test("-n", "-renamed");
        test("n + n", "renamed + renamed");
        test("CAST(n AS TEXT) || 'n'", "CAST(renamed AS TEXT) || 'n'");
        test("DATE '2026-11-07'", "DATE '2026-11-07'");

        assert!(matches!(
            rename_index_column(&mut expr("ABS(n)"), "n", "renamed"),
            Err(StorageError::ConflictRenamingIndexExpression)
        ));
        assert!(matches!(
            rename_index_column(&mut expr("Items.n"), "n", "renamed"),
            Err(StorageError::ConflictRenamingIndexExpression)
        ));
        assert!(matches!(
            rename_index_column(&mut expr("n + ABS(n)"), "n", "renamed"),
            Err(StorageError::ConflictRenamingIndexExpression)
        ));
    }
}
