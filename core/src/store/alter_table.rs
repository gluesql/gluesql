use {
    super::{DataRow, Store, StoreMut},
    crate::{ast::ColumnDef, data::Value, executor::evaluate_stateless, result::Result},
    async_trait::async_trait,
    futures::TryStreamExt,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum AlterTableError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Renaming column not found")]
    RenamingColumnNotFound,

    #[error("Default value is required: {0:#?}")]
    DefaultValueRequired(ColumnDef),

    #[error("Already existing column: {0}")]
    AlreadyExistingColumn(String),

    #[error("Dropping column not found: {0}")]
    DroppingColumnNotFound(String),

    #[error("Schemaless table does not support ALTER TABLE: {0}")]
    SchemalessTableFound(String),

    #[error("conflict - Vec expected but Map row found")]
    ConflictOnUnexpectedMapRowFound,
}

#[async_trait(?Send)]
pub trait AlterTable: Store + StoreMut {
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        schema.table_name = new_table_name.to_owned();
        self.insert_schema(&schema).await?;

        let rows = self
            .scan_data(table_name)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        self.insert_data(new_table_name, rows).await?;
        self.delete_schema(table_name).await
    }

    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let column_defs = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        if column_defs
            .iter()
            .any(|column_def| column_def.name == new_column_name)
        {
            return Err(AlterTableError::AlreadyExistingColumn(new_column_name.to_owned()).into());
        }

        column_defs
            .iter_mut()
            .find(|column_def| column_def.name == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound)?
            .name = new_column_name.to_owned();

        let rows = self
            .scan_data(table_name)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        self.insert_schema(&schema).await?;
        self.insert_data(table_name, rows).await
    }

    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let default_value = match (column_def.default.as_ref(), column_def.nullable) {
            (Some(default), _) => evaluate_stateless(None, default).await?.try_into()?,
            (None, true) => Value::Null,
            (None, false) => {
                return Err(AlterTableError::DefaultValueRequired(column_def.clone()).into());
            }
        };

        let column_defs = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        if column_defs.iter().any(|def| def.name == column_def.name) {
            return Err(AlterTableError::AlreadyExistingColumn(column_def.name.clone()).into());
        }

        column_defs.push(column_def.clone());

        let rows = self
            .scan_data(table_name)
            .await?
            .and_then(|(key, mut data_row)| {
                let default_value = default_value.clone();

                async move {
                    match &mut data_row {
                        DataRow::Map(_) => {
                            Err(AlterTableError::ConflictOnUnexpectedMapRowFound.into())
                        }
                        DataRow::Vec(rows) => {
                            rows.push(default_value);

                            Ok((key, data_row))
                        }
                    }
                }
            })
            .try_collect::<Vec<_>>()
            .await?;

        self.insert_schema(&schema).await?;
        self.insert_data(table_name, rows).await
    }

    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)
            .await?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let column_defs = schema
            .column_defs
            .as_mut()
            .ok_or_else(|| AlterTableError::SchemalessTableFound(table_name.to_owned()))?;

        let i = match column_defs
            .iter()
            .position(|column_def| column_def.name == column_name)
        {
            Some(i) => i,
            None if if_exists => return Ok(()),
            None => {
                return Err(AlterTableError::DroppingColumnNotFound(column_name.to_owned()).into());
            }
        };

        column_defs.retain(|column_def| column_def.name != column_name);

        let rows = self
            .scan_data(table_name)
            .await?
            .and_then(|(key, mut data_row)| async move {
                match &mut data_row {
                    DataRow::Map(_) => Err(AlterTableError::ConflictOnUnexpectedMapRowFound.into()),
                    DataRow::Vec(rows) => {
                        rows.remove(i);

                        Ok((key, data_row))
                    }
                }
            })
            .try_collect::<Vec<_>>()
            .await?;

        self.insert_schema(&schema).await?;
        self.insert_data(table_name, rows).await
    }
}
