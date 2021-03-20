use {
    super::AlterError,
    crate::{
        data::{get_name, Schema},
        result::{MutResult, Result},
        store::{AlterTable, Store, StoreMut},
    },
    sqlparser::ast::{ColumnDef, ColumnOption, DataType, ObjectName},
    std::fmt::Debug,
};

pub async fn create_table<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable>(
    storage: U,
    name: &ObjectName,
    column_defs: &[ColumnDef],
    if_not_exists: bool,
) -> MutResult<U, ()> {
    let schema = (|| async {
        let schema = Schema {
            table_name: get_name(name)?.to_string(),
            column_defs: column_defs.to_vec(),
        };

        validate_column_unique_option(&schema.column_defs)?;
        validate_table_if_not_exists(&storage, &schema.table_name, if_not_exists).await?;

        Ok(schema)
    })()
    .await;

    let schema = match schema {
        Ok(s) => s,
        Err(e) => {
            return Err((storage, e));
        }
    };

    storage.insert_schema(&schema).await
}

fn validate_column_unique_option(column_defs: &[ColumnDef]) -> Result<()> {
    let found = column_defs.iter().find(|col| match col.data_type {
        DataType::Float(_) => col
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::Unique { .. })),
        _ => false,
    });

    if let Some(col) = found {
        return Err(AlterError::UnsupportedDataTypeForUniqueColumn(
            col.name.to_string(),
            col.data_type.to_string(),
        )
        .into());
    }

    Ok(())
}

async fn validate_table_if_not_exists<'a, T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    if_not_exists: bool,
) -> Result<()> {
    if if_not_exists || storage.fetch_schema(table_name).await?.is_none() {
        return Ok(());
    }

    Err(AlterError::TableAlreadyExists(table_name.to_owned()).into())
}
