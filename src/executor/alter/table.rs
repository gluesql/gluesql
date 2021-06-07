use {
    super::{validate, AlterError},
    crate::{
        ast::{ColumnDef, ObjectName},
        data::{get_name, Schema},
        result::MutResult,
        store::{GStore, GStoreMut},
    },
    futures::stream::{self, TryStreamExt},
    std::fmt::Debug,
};

pub async fn create_table<T: 'static + Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    name: &ObjectName,
    column_defs: &[ColumnDef],
    if_not_exists: bool,
) -> MutResult<U, ()> {
    let schema = (|| async {
        let schema = Schema {
            table_name: get_name(name)?.to_string(),
            column_defs: column_defs.to_vec(),
            indexes: vec![],
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

pub async fn drop_table<T: 'static + Debug, U: GStore<T> + GStoreMut<T>>(
    storage: U,
    table_names: &[ObjectName],
    if_exists: bool,
) -> MutResult<U, ()> {
    stream::iter(table_names.iter().map(Ok))
        .try_fold((storage, ()), |(storage, _), table_name| async move {
            let schema = (|| async {
                let table_name = get_name(table_name)?;
                let schema = storage.fetch_schema(table_name).await?;

                if !if_exists {
                    schema.ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
                }

                Ok(table_name)
            })()
            .await;

            let schema = match schema {
                Ok(s) => s,
                Err(e) => {
                    return Err((storage, e));
                }
            };

            storage.delete_schema(schema).await
        })
        .await
}
