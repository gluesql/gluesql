use {
    super::select::select,
    crate::{
        ast::{ColumnDef, Query, SetExpr, Values},
        data::Row,
        result::Result,
        store::{GStore, GStoreMut},
    },
    futures::stream::TryStreamExt,
    std::{fmt::Debug, rc::Rc},
};
pub enum Queried {
    Select(Vec<Row>),
    Values(Vec<Row>),
}

pub async fn query<T: Debug, U: GStore<T> + GStoreMut<T>>(
    source: &Query,
    column_defs: Rc<[ColumnDef]>,
    columns: &[String],
    storage: &U,
) -> Result<Queried> {
    match &source.body {
        SetExpr::Values(Values(values_list)) => {
            let values = values_list
                .iter()
                .map(|values| Row::new(&column_defs, columns, values))
                .collect::<Result<Vec<Row>>>()?;
            Ok(Queried::Values(values))
        }
        SetExpr::Select(select_query) => {
            let selected = select(storage, &source, None)
                .await?
                .and_then(|row| {
                    let column_defs = Rc::clone(&column_defs);

                    async move {
                        row.validate(&column_defs)?;

                        Ok(row)
                    }
                })
                .try_collect::<Vec<_>>()
                .await?;
            Ok(Queried::Select(selected))
        }
    }
}
