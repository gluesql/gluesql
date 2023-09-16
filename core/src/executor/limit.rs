use {
    super::evaluate::evaluate_stateless,
    crate::{
        ast::Expr,
        data::{Row, Value},
        result::{Error, Result},
    },
    futures::stream::{Stream, StreamExt},
};

pub struct Limit {
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Limit {
    pub async fn new(limit: Option<&Expr>, offset: Option<&Expr>) -> Result<Self> {
        let eval = |expr| async move {
            let expr = match expr {
                Some(expr) => expr,
                None => return Ok(None),
            };

            let evaluated = evaluate_stateless(None, expr).await?;
            let size: usize = Value::try_from(evaluated)?.try_into()?;

            Result::<Option<usize>, Error>::Ok(Some(size))
        };

        let limit = eval(limit).await?;
        let offset = eval(offset).await?;

        Ok(Self { limit, offset })
    }

    pub fn apply<'a>(
        &self,
        rows: impl Stream<Item = Result<Row>> + 'a,
    ) -> impl Stream<Item = Result<Row>> + 'a {
        #[derive(futures_enum::Stream)]
        enum S<S1, S2, S3, S4> {
            Both(S3),
            Offset(S2),
            Limit(S1),
            None(S4),
        }

        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => S::Both(rows.skip(offset).take(limit)),
            (Some(offset), None) => S::Offset(rows.skip(offset)),
            (None, Some(limit)) => S::Limit(rows.take(limit)),
            (None, None) => S::None(rows),
        }
    }
}
