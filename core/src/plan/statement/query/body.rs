use {
    super::{LimitInputPlan, LimitPlan, OffsetPlan, QueryPlan},
    crate::{
        ast,
        plan::{AggregatePlan, ExprPlan, ProjectionPlan, TableWithJoinsPlan},
    },
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryBodyPlan {
    pub body: SetExprPlan,
    pub order_by: Vec<OrderByExprPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SetExprPlan {
    Select(Box<SelectPlan>),
    Values(ValuesPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectPlan {
    pub distinct: bool,
    pub projection: ProjectionPlan,
    pub from: TableWithJoinsPlan,
    pub selection: Option<ExprPlan>,
    pub group_by: Vec<ExprPlan>,
    pub having: Option<ExprPlan>,
    pub aggregate_slots: Option<Vec<AggregatePlan>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExprPlan {
    pub expr: ExprPlan,
    pub asc: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValuesPlan(pub Vec<Vec<ExprPlan>>);

impl QueryPlan {
    fn body_plan(&self) -> &QueryBodyPlan {
        match self {
            Self::Body(body) => body,
            Self::Offset(OffsetPlan { input, .. }) => input,
            Self::Limit(LimitPlan { input, .. }) => match input {
                LimitInputPlan::Body(body) => body,
                LimitInputPlan::Offset(OffsetPlan { input, .. }) => input,
            },
        }
    }

    pub fn body(&self) -> &SetExprPlan {
        &self.body_plan().body
    }

    pub fn order_by(&self) -> &[OrderByExprPlan] {
        &self.body_plan().order_by
    }
}

impl From<ast::SetExpr> for SetExprPlan {
    fn from(set_expr: ast::SetExpr) -> Self {
        match set_expr {
            ast::SetExpr::Select(select) => Self::Select(Box::new((*select).into())),
            ast::SetExpr::Values(values) => Self::Values(values.into()),
        }
    }
}

impl From<ast::Select> for SelectPlan {
    fn from(select: ast::Select) -> Self {
        let ast::Select {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
        } = select;

        Self {
            distinct,
            projection: projection.into(),
            from: from.into(),
            selection: selection.map(Into::into),
            group_by: group_by.into_iter().map(Into::into).collect(),
            having: having.map(Into::into),
            aggregate_slots: None,
        }
    }
}

impl From<ast::OrderByExpr> for OrderByExprPlan {
    fn from(order_by: ast::OrderByExpr) -> Self {
        let ast::OrderByExpr { expr, asc } = order_by;

        Self {
            expr: expr.into(),
            asc,
        }
    }
}

impl From<ast::Values> for ValuesPlan {
    fn from(values: ast::Values) -> Self {
        Self(
            values
                .0
                .into_iter()
                .map(|exprs| exprs.into_iter().map(Into::into).collect())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{QueryBodyPlan, SetExprPlan, ValuesPlan},
        crate::{
            ast::Literal,
            plan::{ExprPlan, LimitInputPlan, LimitPlan, OffsetPlan, OrderByExprPlan, QueryPlan},
        },
    };

    fn body() -> QueryBodyPlan {
        QueryBodyPlan {
            body: SetExprPlan::Values(ValuesPlan(Vec::new())),
            order_by: vec![OrderByExprPlan {
                expr: count(),
                asc: None,
            }],
        }
    }

    fn count() -> ExprPlan {
        ExprPlan::Literal(Literal::Number(1.into()))
    }

    #[test]
    fn query_plan_body_follows_every_state() {
        let plans = [
            QueryPlan::Body(body()),
            QueryPlan::Offset(OffsetPlan {
                input: body(),
                count: count(),
            }),
            QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Body(body()),
                count: count(),
            }),
            QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(OffsetPlan {
                    input: body(),
                    count: count(),
                }),
                count: count(),
            }),
        ];

        for plan in plans {
            assert!(matches!(plan.body(), SetExprPlan::Values(_)));
            assert_eq!(plan.order_by().len(), 1);
        }
    }
}
