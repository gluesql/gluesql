use {
    crate::{
        Result, SqliteStorage, SqliteStorageError,
        codec::encode,
        schema::{COLUMN_META, TABLE_META, columns, is_reserved, quote, rowid_name},
    },
    gluesql_core::{
        ast::ColumnDef,
        data::Value,
        error::{AlterTableError, Result as GlueResult},
        store::AlterTable,
    },
    rusqlite::params,
};

impl AlterTable for SqliteStorage {
    fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> GlueResult<()> {
        self.atomic(|storage| storage.rename_table(table_name, new_table_name))
            .map_err(Into::into)
    }

    fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> GlueResult<()> {
        self.atomic(|storage| {
            storage.rename_table_column(table_name, old_column_name, new_column_name)
        })
        .map_err(Into::into)
    }

    fn add_column(
        &mut self,
        table_name: &str,
        column_def: &ColumnDef,
        default_value: Value,
    ) -> GlueResult<()> {
        self.atomic(|storage| storage.add_table_column(table_name, column_def, &default_value))
            .map_err(Into::into)
    }

    fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> GlueResult<()> {
        self.atomic(|storage| storage.drop_table_column(table_name, column_name, if_exists))
            .map_err(Into::into)
    }
}

impl SqliteStorage {
    fn rename_table(&self, table_name: &str, new_table_name: &str) -> Result<()> {
        let schema = self
            .read_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        if is_reserved(new_table_name) {
            return Err(SqliteStorageError::ReservedTableName(
                new_table_name.to_owned(),
            ));
        }

        self.connection.execute_batch(&format!(
            "ALTER TABLE {} RENAME TO {}",
            quote(&schema.table_name),
            quote(new_table_name)
        ))?;

        for table in [TABLE_META, COLUMN_META] {
            self.connection.execute(
                &format!("UPDATE {table} SET table_name = ?1 WHERE table_name = ?2"),
                params![new_table_name, schema.table_name],
            )?;
        }

        Ok(())
    }

    fn rename_table_column(
        &self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let mut schema = self
            .read_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let columns = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        if columns.iter().any(|column| column.name == new_column_name) {
            return Err(AlterTableError::AlreadyExistingColumn(new_column_name.to_owned()).into());
        }

        let column = columns
            .iter_mut()
            .find(|column| column.name == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound)?;
        new_column_name.clone_into(&mut column.name);
        rowid_name(&schema)?;

        self.connection.execute_batch(&format!(
            "ALTER TABLE {} RENAME COLUMN {} TO {}",
            quote(&schema.table_name),
            quote(old_column_name),
            quote(new_column_name)
        ))?;

        self.connection.execute(
            "UPDATE __gluesql_column_meta
             SET column_name = ?1 WHERE table_name = ?2 AND column_name = ?3",
            params![new_column_name, schema.table_name, old_column_name],
        )?;

        Ok(())
    }

    fn add_table_column(
        &self,
        table_name: &str,
        column_def: &ColumnDef,
        default_value: &Value,
    ) -> Result<()> {
        let old = self
            .read_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let mut new = old.clone();
        let column_defs = new
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        if column_defs
            .iter()
            .any(|column| column.name == column_def.name)
        {
            return Err(AlterTableError::AlreadyExistingColumn(column_def.name.clone()).into());
        }

        let mut names = column_defs
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        column_defs.push(column_def.clone());
        rowid_name(&new)?;

        let default_value = encode(default_value, &column_def.data_type)?;
        let mut rows = self.read_rows_for_rebuild(&old, &names)?;
        for row in &mut rows {
            row.push(default_value.clone());
        }

        names.push(column_def.name.clone());
        self.recreate_table(&new, &names, &rows)?;
        self.write_column_metadata(&new.table_name, &columns(&new))
    }

    fn drop_table_column(
        &self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let mut schema = self
            .read_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        let columns = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        let Some(index) = columns.iter().position(|column| column.name == column_name) else {
            return if if_exists {
                Ok(())
            } else {
                Err(AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into())
            };
        };

        columns.remove(index);
        self.write_schema(&schema)
    }
}
