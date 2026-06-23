use {
    super::{Store, StoreMut},
    crate::{
        ast::ColumnDef, data::Value, executor::evaluate_stateless, plan::plan_scalar_expr,
        result::Result,
    },
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
}

pub trait AlterTable: Store + StoreMut {
    fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;
        new_table_name.clone_into(&mut schema.table_name);
        self.insert_schema(&schema)?;

        let rows = self.scan_data(table_name)?.collect::<Result<Vec<_>>>()?;

        self.insert_data(new_table_name, rows)?;
        self.delete_schema(table_name)
    }

    fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)?
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

        new_column_name.clone_into(
            &mut column_defs
                .iter_mut()
                .find(|column_def| column_def.name == old_column_name)
                .ok_or(AlterTableError::RenamingColumnNotFound)?
                .name,
        );

        let rows = self.scan_data(table_name)?.collect::<Result<Vec<_>>>()?;

        self.insert_schema(&schema)?;
        self.insert_data(table_name, rows)
    }

    fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)?
            .ok_or_else(|| AlterTableError::TableNotFound(table_name.to_owned()))?;

        let default_value = match (column_def.default.as_ref(), column_def.nullable) {
            (Some(default), _) => {
                let default = plan_scalar_expr(default.clone());

                evaluate_stateless(None, &default)?.try_into()?
            }
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
            .scan_data(table_name)?
            .map(|row| {
                let (key, mut values) = row?;
                let default_value = default_value.clone();

                values.push(default_value);
                Ok((key, values))
            })
            .collect::<Result<Vec<_>>>()?;

        self.insert_schema(&schema)?;
        self.insert_data(table_name, rows)
    }

    fn drop_column(&mut self, table_name: &str, column_name: &str, if_exists: bool) -> Result<()> {
        let mut schema = self
            .fetch_schema(table_name)?
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
            .scan_data(table_name)?
            .map(|row| {
                let (key, mut values) = row?;
                values.remove(i);
                Ok((key, values))
            })
            .collect::<Result<Vec<_>>>()?;

        self.insert_schema(&schema)?;
        self.insert_data(table_name, rows)
    }
}
