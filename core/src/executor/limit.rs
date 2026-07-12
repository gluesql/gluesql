use {
    super::evaluate::evaluate_stateless,
    crate::{
        data::{Row, Value},
        plan::{ExprPlan, LimitInputPlan, LimitPlan, OffsetPlan, QueryPlan},
        result::{Error, Result},
    },
};

pub struct Limit(usize);

pub struct Offset(usize);

impl Limit {
    pub fn new(plan: &LimitPlan) -> Result<Self> {
        evaluate_count(&plan.count).map(Self)
    }

    pub fn apply<'a, T: Iterator<Item = Result<Row>> + 'a>(
        &self,
        rows: T,
    ) -> Box<dyn Iterator<Item = Result<Row>> + 'a> {
        Box::new(rows.take(self.0))
    }
}

impl Offset {
    pub fn new(plan: &OffsetPlan) -> Result<Self> {
        evaluate_count(&plan.count).map(Self)
    }

    pub fn apply<'a, T: Iterator<Item = Result<Row>> + 'a>(
        &self,
        rows: T,
    ) -> Box<dyn Iterator<Item = Result<Row>> + 'a> {
        Box::new(rows.skip(self.0))
    }
}

fn evaluate_count(expr: &ExprPlan) -> Result<usize> {
    let evaluated = evaluate_stateless(None, expr)?;
    let size: usize = Value::try_from(evaluated)?.try_into()?;

    Result::<usize, Error>::Ok(size)
}

pub fn apply<'a, T: Iterator<Item = Result<Row>> + 'a>(
    query: &QueryPlan,
    rows: T,
) -> Result<Box<dyn Iterator<Item = Result<Row>> + 'a>> {
    match query {
        QueryPlan::Body(_) | QueryPlan::OrderBy(_) => Ok(Box::new(rows)),
        QueryPlan::Offset(plan) => {
            let offset = Offset::new(plan)?;

            Ok(offset.apply(rows))
        }
        QueryPlan::Limit(plan) => {
            let rows: Box<dyn Iterator<Item = Result<Row>> + 'a> = match &plan.input {
                LimitInputPlan::Body(_) | LimitInputPlan::OrderBy(_) => Box::new(rows),
                LimitInputPlan::Offset(offset_plan) => Offset::new(offset_plan)?.apply(rows),
            };
            let limit = Limit::new(plan)?;

            Ok(limit.apply(rows))
        }
    }
}
