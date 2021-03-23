use {
    super::{validate, AlterError},
    crate::{
        data::{get_name, Schema},
        result::MutResult,
        store::{AlterTable, Store, StoreMut},
    },
    sqlparser::ast::{ColumnDef, ObjectName},
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

        for column_def in &schema.column_defs {
            validate(column_def)?;
        }

        if !if_not_exists && storage.fetch_schema(&schema.table_name).await?.is_some() {
            return Err(AlterError::TableAlreadyExists(schema.table_name.to_owned()).into());
        }

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
