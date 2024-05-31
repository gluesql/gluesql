use {
    super::{
        fetch::{fetch, fetch_columns},
        ExecuteError, Payload, Referencing,
    },
    crate::{
        ast::{BinaryOperator, Expr, ForeignKey, ReferentialAction},
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    futures::stream::{StreamExt, TryStreamExt},
    std::rc::Rc,
};

pub async fn delete<T: GStore + GStoreMut>(
    storage: &mut T,
    table_name: &str,
    selection: &Option<Expr>,
) -> Result<Payload> {
    let columns = fetch_columns(storage, table_name).await?.map(Rc::from);
    let referencings = storage.fetch_referencings(table_name).await?;
    let keys =
        fetch(storage, table_name, columns, selection.as_ref())
            .await?
            .into_stream()
            .then(|item| async {
                let (key, row) = item?;

                for Referencing {
                    table_name: referencing_table_name,
                    foreign_key:
                        ForeignKey {
                            referencing_column_name,
                            referenced_column_name,
                            on_delete,
                            ..
                        },
                } in &referencings
                {
                    let value = row.get_value(referenced_column_name).ok_or(
                        ExecuteError::ValueNotFound(referenced_column_name.to_owned()),
                    )?;

                    let expr = &Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(referencing_column_name.clone())),
                        op: BinaryOperator::Eq,
                        right: Box::new(value.to_owned().try_into()?),
                    };

                    let columns = Some(vec![referencing_column_name.to_owned()]).map(Rc::from);
                    let referencing_rows =
                        fetch(storage, referencing_table_name, columns, Some(expr)).await?;

                    let len = referencing_rows.count().await;
                    if len > 0 && on_delete == &ReferentialAction::NoAction {
                        return Err(ExecuteError::ReferencingColumnExists(format!(
                            "{referencing_table_name}.{referencing_column_name}"
                        ))
                        .into());
                    }
                }

                Ok::<_, Error>(key)
            })
            .try_collect::<Vec<_>>()
            .await?;
    let num_keys = keys.len();

    storage
        .delete_data(table_name, keys)
        .await
        .map(|_| Payload::Delete(num_keys))
}
