#![cfg(feature = "alter-table")]

use {
    super::MemoryStorage,
    crate::{
        ast::ColumnDef,
        result::MutResult,
        schema::ColumnDefExt,
        store::AlterTable,
        store::{Store, StoreMut},
        AlterTableError, Row, Value,
    },
    async_trait::async_trait,
};

#[async_trait(?Send)]
impl AlterTable for MemoryStorage {
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        let mut storage = self;

        let mut item = storage
            .items
            .remove(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
            .map_err(|err| err_into(&storage, err))?;
        item.schema.table_name = new_table_name.to_owned();
        storage.items.insert(new_table_name.to_owned(), item);

        Ok((storage, ()))
    }

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let mut storage = self;

        let ms = storage.clone();
        let item = storage
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
            .map_err(|err| err_into(&ms, err))?;
        let column_def = item
            .schema
            .column_defs
            .iter_mut()
            .find(|column_def| column_def.name == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound)
            .map_err(|err| err_into(&ms, err))?;
        column_def.name = new_column_name.to_owned();

        Ok((storage, ()))
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let storage = self;

        let ms = storage.clone();
        let item = storage
            .items
            .get(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
            .map_err(|err| err_into(&ms, err))?;

        if item
            .schema
            .column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name == &column_def.name)
        {
            let adding_column = column_def.name.to_owned();

            return Err((
                ms.clone(),
                AlterTableError::AddingColumnAlreadyExists(adding_column).into(),
            ));
        }

        let ColumnDef { data_type, .. } = column_def;
        let nullable = column_def.is_nullable();
        let default = column_def.get_default();
        let value = match (default, nullable) {
            (Some(expr), _) => {
                let evaluated =
                    crate::evaluate_stateless(None, expr).map_err(|err| err_into(&ms, err))?;

                evaluated
                    .try_into_value(data_type, nullable)
                    .map_err(|err| err_into(&ms, err))?
            }
            (None, true) => Value::Null,
            (None, false) => {
                return Err((
                    ms.clone(),
                    AlterTableError::DefaultValueRequired(column_def.clone()).into(),
                ))
            }
        };

        let rows = storage
            .scan_data(table_name)
            .await
            .map_err(|err| err_into(&ms, err))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err_into(&ms, err))?
            .into_iter()
            .map(|(key, mut row)| {
                row.0.push(value.clone());
                (key, row)
            })
            .collect::<Vec<_>>();

        let mut storage = storage.update_data(table_name, rows).await?.0;

        let item = storage
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
            .map_err(|err| err_into(&ms, err))?;
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

        let ms = storage.clone();
        let item = storage
            .items
            .get_mut(table_name)
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))
            .map_err(|err| (ms.clone(), err.into()))?;
        let column_index = item
            .schema
            .column_defs
            .iter()
            .position(|column_def| column_def.name == column_name);

        let rows = match column_index {
            Some(column_index) => {
                item.schema.column_defs.remove(column_index);

                storage
                    .scan_data(table_name)
                    .await
                    .map_err(|err| err_into(&ms, err))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| err_into(&ms, err))?
                    .into_iter()
                    .map(|(key, row)| {
                        let row = Row(row
                            .0
                            .into_iter()
                            .enumerate()
                            .filter_map(|(i, v)| (i != column_index).then(|| v))
                            .collect());

                        (key, row)
                    })
                    .collect::<Vec<_>>()
            }
            None if if_exists => {
                vec![]
            }
            None => {
                return Err((
                    storage,
                    AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into(),
                ))
            }
        };

        let storage = storage.update_data(table_name, rows).await?.0;

        Ok((storage, ()))
    }
}

fn err_into(ms: &MemoryStorage, err: impl Into<crate::Error>) -> (MemoryStorage, crate::Error) {
    (ms.clone(), err.into())
}
