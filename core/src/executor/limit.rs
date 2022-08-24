use {
    super::evaluate::evaluate_stateless,
    crate::{
        ast::Expr,
        data::{Row, Value},
        result::Result,
    },
    futures::stream::{Stream, StreamExt},
    std::pin::Pin,
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
    ) -> Pin<Box<dyn Stream<Item = Result<Row>> + 'a>> {
        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => Box::pin(rows.skip(offset).take(limit)),
            (Some(offset), None) => Box::pin(rows.skip(offset)),
            (None, Some(limit)) => Box::pin(rows.take(limit)),
            (None, None) => Box::pin(rows),
        }
    }
}
