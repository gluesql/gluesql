use {
    super::context::BlendContext,
    crate::{
        ast::{AstLiteral, Expr},
        result::Result,
    },
    futures::stream::{Stream, StreamExt},
    serde::Serialize,
    std::{fmt::Debug, pin::Pin, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq)]
pub enum LimitError {
    #[error("Unreachable")]
    Unreachable,
}

pub struct Limit {
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Limit {
    pub fn new(limit: Option<&Expr>, offset: Option<&Expr>) -> Result<Self> {
        let parse = |expr: &Expr| -> Result<usize> {
            match expr {
                Expr::Literal(AstLiteral::Number(v)) => {
                    v.parse().map_err(|_| LimitError::Unreachable.into())
                }
                _ => Err(LimitError::Unreachable.into()),
            }
        };

        let limit = limit.map(parse).transpose()?;
        let offset = offset.map(parse).transpose()?;

        Ok(Self { limit, offset })
    }

    pub fn apply<'a>(
        &self,
        rows: impl Stream<Item = Result<Rc<BlendContext<'a>>>> + 'a,
    ) -> Pin<Box<dyn Stream<Item = Result<Rc<BlendContext<'a>>>> + 'a>> {
        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => Box::pin(rows.skip(offset).take(limit)),
            (Some(offset), None) => Box::pin(rows.skip(offset)),
            (None, Some(limit)) => Box::pin(rows.take(limit)),
            (None, None) => Box::pin(rows),
        }
    }
}
