use {
    super::AlterError,
    crate::{
        ast::ObjectName,
        data::get_name,
        result::MutResult,
        store::{AlterTable, Store, StoreMut},
    },
    futures::stream::{self, TryStreamExt},
    std::fmt::Debug,
};

pub async fn drop<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable>(
    storage: U,
    names: &[ObjectName],
    if_exists: bool,
) -> MutResult<U, ()> {
    stream::iter(names.iter().map(Ok))
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
