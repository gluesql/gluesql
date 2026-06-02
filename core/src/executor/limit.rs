use {
    super::evaluate::evaluate_stateless,
    crate::{
        data::{Row, Value},
        plan::ExprPlan,
        result::{Error, Result},
    },
};

pub struct Limit {
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Limit {
    pub fn new(limit: Option<&ExprPlan>, offset: Option<&ExprPlan>) -> Result<Self> {
        let eval = |expr| {
            let Some(expr) = expr else {
                return Ok(None);
            };

            let evaluated = evaluate_stateless(None, expr)?;
            let size: usize = Value::try_from(evaluated)?.try_into()?;

            Result::<Option<usize>, Error>::Ok(Some(size))
        };

        let limit = eval(limit)?;
        let offset = eval(offset)?;

        Ok(Self { limit, offset })
    }

    pub fn apply<'a, T: Iterator<Item = Result<Row>> + Send + 'a>(
        &self,
        rows: T,
    ) -> Box<dyn Iterator<Item = Result<Row>> + Send + 'a> {
        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => Box::new(rows.skip(offset).take(limit)),
            (Some(offset), None) => Box::new(rows.skip(offset)),
            (None, Some(limit)) => Box::new(rows.take(limit)),
            (None, None) => Box::new(rows),
        }
    }
}
