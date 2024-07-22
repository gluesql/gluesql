use {
    super::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        ast::ColumnDef,
        data::Value,
        error::{AlterTableError, Error, Result},
        store::{AlterTable, DataRow},
    },
};

#[async_trait(?Send)]
impl AlterTable for MemoryStorage {
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        let mut item = self
            .items
            .remove(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        item.schema.table_name = new_table_name.to_owned();
        self.items.insert(new_table_name.to_owned(), item);

        Ok(())
    }

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let item = self
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let column_defs = item
            .schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        if column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name == new_column_name)
        {
            return Err(AlterTableError::AlreadyExistingColumn(new_column_name.to_owned()).into());
        }

        let column_def = column_defs
            .iter_mut()
            .find(|column_def| column_def.name == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound)?;

        column_def.name = new_column_name.to_owned();

        Ok(())
    }

    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        let item = self
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let column_defs = item
            .schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        if column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name == &column_def.name)
        {
            let adding_column = column_def.name.to_owned();

            return Err(AlterTableError::AlreadyExistingColumn(adding_column).into());
        }

        let ColumnDef {
            data_type,
            nullable,
            default,
            ..
        } = column_def;

        let value = match (default, nullable) {
            (Some(expr), _) => {
                let evaluated = gluesql_core::executor::evaluate_stateless(None, expr).await?;

                evaluated.try_into_value(data_type, *nullable)?
            }
            (None, true) => Value::Null,
            (None, false) => {
                return Err(AlterTableError::DefaultValueRequired(column_def.clone()).into())
            }
        };

        for (_, row) in item.rows.iter_mut() {
            match row {
                DataRow::Vec(values) => {
                    values.push(value.clone());
                }
                DataRow::Map(_) => {
                    return Err(Error::StorageMsg(
                        "conflict - add_column failed: schemaless row found".to_owned(),
                    ));
                }
            }
        }

        column_defs.push(column_def.clone());

        Ok(())
    }

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let item = self
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let column_defs = item
            .schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        let column_index = column_defs
            .iter()
            .position(|column_def| column_def.name == column_name);

        match column_index {
            Some(column_index) => {
                column_defs.remove(column_index);

                for (_, row) in item.rows.iter_mut() {
                    if row.len() <= column_index {
                        continue;
                    }

                    match row {
                        DataRow::Vec(values) => {
                            values.remove(column_index);
                        }
                        DataRow::Map(_) => {
                            return Err(Error::StorageMsg(
                                "conflict - drop_column failed: schemaless row found".to_owned(),
                            ));
                        }
                    }
                }
            }
            None if if_exists => {}
            None => {
                return Err(AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into())
            }
        };

        Ok(())
    }
}
