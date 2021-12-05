#![cfg(feature = "alter-table")]

use {
    super::MemoryStorage,
    crate::{
        ast::ColumnDef,
        data::{schema::ColumnDefExt, Value},
        result::Result,
        result::{MutResult, TrySelf},
        store::AlterTable,
        store::AlterTableError,
    },
    async_trait::async_trait,
};

impl MemoryStorage {
    fn rename_column_sync(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let item = self
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let mut column_def = item
            .schema
            .column_defs
            .iter_mut()
            .find(|column_def| column_def.name == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound)?;

        column_def.name = new_column_name.to_owned();

        Ok(())
    }

    fn add_column_sync(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        let item = self
            .items
            .get(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        if item
            .schema
            .column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name == &column_def.name)
        {
            let adding_column = column_def.name.to_owned();

            return Err(AlterTableError::AddingColumnAlreadyExists(adding_column).into());
        }

        let ColumnDef { data_type, .. } = column_def;
        let nullable = column_def.is_nullable();
        let default = column_def.get_default();
        let value = match (default, nullable) {
            (Some(expr), _) => {
                let evaluated = crate::executor::evaluate_stateless(None, expr)?;

                evaluated.try_into_value(data_type, nullable)?
            }
            (None, true) => Value::Null,
            (None, false) => {
                return Err(AlterTableError::DefaultValueRequired(column_def.clone()).into())
            }
        };

        let item = self
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        item.rows.iter_mut().for_each(|(_, row)| {
            row.0.push(value.clone());
        });
        item.schema.column_defs.push(column_def.clone());

        Ok(())
    }

    fn drop_column_sync(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let item = self
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let column_index = item
            .schema
            .column_defs
            .iter()
            .position(|column_def| column_def.name == column_name);

        match column_index {
            Some(column_index) => {
                item.schema.column_defs.remove(column_index);

                item.rows.iter_mut().for_each(|(_, row)| {
                    if row.0.len() > column_index {
                        row.0.remove(column_index);
                    }
                });
            }
            None if if_exists => {}
            None => {
                return Err(AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into())
            }
        };

        Ok(())
    }
}

#[async_trait(?Send)]
impl AlterTable for MemoryStorage {
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        let mut storage = self;

        match storage
            .items
            .remove(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
        {
            Ok(mut item) => {
                item.schema.table_name = new_table_name.to_owned();
                storage.items.insert(new_table_name.to_owned(), item);

                Ok((storage, ()))
            }
            Err(err) => Err((storage, err.into())),
        }
    }

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let mut storage = self;

        storage
            .rename_column_sync(table_name, old_column_name, new_column_name)
            .try_self(storage)
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let mut storage = self;

        storage
            .add_column_sync(table_name, column_def)
            .try_self(storage)
    }

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()> {
        let mut storage = self;

        storage
            .drop_column_sync(table_name, column_name, if_exists)
            .try_self(storage)
    }
}
