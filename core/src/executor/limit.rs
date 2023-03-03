use {
    super::evaluate::evaluate_stateless,
    crate::{
        ast::Expr,
        data::{Row, Value},
        result::Result,
    },
    futures::stream::{Stream, StreamExt},
};

pub struct Limit {
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Limit {
    pub fn new(limit: Option<&Expr>, offset: Option<&Expr>) -> Result<Self> {
        let eval = |expr| -> Result<usize> {
            let value: Value = evaluate_stateless(None, expr)?.try_into()?;

            value.try_into()
        };

        let limit = limit.map(eval).transpose()?;
        let offset = offset.map(eval).transpose()?;

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
