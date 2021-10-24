#![cfg(feature = "alter-table")]

use {
    super::MemoryStorage,
    crate::{
        ast::ColumnDef, result::MutResult, schema::ColumnDefExt, store::AlterTable,
        AlterTableError, Value,
    },
    async_trait::async_trait,
};

macro_rules! unwrap_or_return_err {
    ($storage:expr, $expr:expr) => {
        match $expr {
            Ok(success) => success,
            Err(err) => return Err(($storage, err.into())),
        }
    };
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

        let item = unwrap_or_return_err!(
            storage,
            storage
                .items
                .get_mut(table_name)
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
        );

        let column_def = unwrap_or_return_err!(
            storage,
            item.schema
                .column_defs
                .iter_mut()
                .find(|column_def| column_def.name == old_column_name)
                .ok_or(AlterTableError::RenamingColumnNotFound)
        );
        column_def.name = new_column_name.to_owned();

        Ok((storage, ()))
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let mut storage = self;

        let item = unwrap_or_return_err!(
            storage,
            storage
                .items
                .get(table_name)
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
        );

        if item
            .schema
            .column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name == &column_def.name)
        {
            let adding_column = column_def.name.to_owned();

            return Err((
                storage,
                AlterTableError::AddingColumnAlreadyExists(adding_column).into(),
            ));
        }

        let ColumnDef { data_type, .. } = column_def;
        let nullable = column_def.is_nullable();
        let default = column_def.get_default();
        let value = match (default, nullable) {
            (Some(expr), _) => {
                let evaluated =
                    unwrap_or_return_err!(storage, crate::evaluate_stateless(None, expr));

                unwrap_or_return_err!(storage, evaluated.try_into_value(data_type, nullable))
            }
            (None, true) => Value::Null,
            (None, false) => {
                return Err((
                    storage,
                    AlterTableError::DefaultValueRequired(column_def.clone()).into(),
                ))
            }
        };

        let item = unwrap_or_return_err!(
            storage,
            storage
                .items
                .get_mut(table_name)
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
        );
        item.rows.iter_mut().for_each(|(_, row)| {
            row.0.push(value.clone());
        });
        item.schema.column_defs.push(column_def.clone());

        Ok((storage, ()))
    }

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()> {
        let mut storage = self;

        let item = unwrap_or_return_err!(
            storage,
            storage
                .items
                .get_mut(table_name)
                .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
        );
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
                return Err((
                    storage,
                    AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into(),
                ))
            }
        };

        Ok((storage, ()))
    }
}
