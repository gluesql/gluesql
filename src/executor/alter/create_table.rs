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

        match (
            storage.fetch_schema(&schema.table_name).await?,
            if_not_exists,
        ) {
            (None, _) => Ok(Some(schema)),
            (Some(_), true) => Ok(None),
            (Some(_), false) => {
                Err(AlterError::TableAlreadyExists(schema.table_name.to_owned()).into())
            }
        }
    })()
    .await;

    let schema = match schema {
        Ok(s) => s,
        Err(e) => {
            return Err((storage, e));
        }
    };

    if let Some(schema) = schema {
        storage.insert_schema(&schema).await
    } else {
        Ok((storage, ()))
    }
}
