use {
    super::{context::FilterContext, filter::check_expr},
    crate::{
        ast::{ColumnDef, Expr},
        data::Row,
        result::{Error, Result},
        store::Store,
    },
    boolinator::Boolinator,
    futures::stream::{self, TryStream, TryStreamExt},
    serde::Serialize,
    std::{fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum FetchError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

pub async fn fetch_columns<T: 'static + Debug>(
    storage: &dyn Store<T>,
    table_name: &str,
) -> Result<Vec<String>> {
    Ok(storage
        .fetch_schema(table_name)
        .await?
        .ok_or_else(|| FetchError::TableNotFound(table_name.to_string()))?
        .column_defs
        .into_iter()
        .map(|ColumnDef { name, .. }| name)
        .collect::<Vec<String>>())
}

pub async fn fetch<'a, T: 'static + Debug>(
    storage: &'a dyn Store<T>,
    table_name: &'a str,
    columns: Rc<[String]>,
    where_clause: Option<&'a Expr>,
) -> Result<impl TryStream<Ok = (Rc<[String]>, T, Row), Error = Error> + 'a> {
    let rows = storage
        .scan_data(table_name)
        .await
        .map(stream::iter)?
        .try_filter_map(move |(key, row)| {
            let columns = Rc::clone(&columns);

            async move {
                let expr = match where_clause {
                    None => {
                        return Ok(Some((columns, key, row)));
                    }
                    Some(expr) => expr,
                };

                let context = FilterContext::new(table_name, Rc::clone(&columns), Some(&row), None);

                check_expr(storage, Some(Rc::new(context)), None, expr)
                    .await
                    .map(|pass| pass.as_some((columns, key, row)))
            }
        });

    Ok(rows)
}
