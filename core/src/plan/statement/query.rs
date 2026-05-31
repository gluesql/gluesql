use {
    super::{AggregatePlan, ExprPlan, ProjectionPlan, TableWithJoinsPlan},
    crate::ast,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryPlan {
    pub body: SetExprPlan,
    pub order_by: Vec<OrderByExprPlan>,
    pub limit: Option<ExprPlan>,
    pub offset: Option<ExprPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SetExprPlan {
    Select(Box<SelectPlan>),
    Values(ValuesPlan),
    Union {
        left: Box<SetExprPlan>,
        right: Box<SetExprPlan>,
        all: bool,
    },
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

impl From<ast::Query> for QueryPlan {
    fn from(query: ast::Query) -> Self {
        let ast::Query {
            body,
            order_by,
            limit,
            offset,
        } = query;

        Self {
            body: body.into(),
            order_by: order_by.into_iter().map(Into::into).collect(),
            limit: limit.map(Into::into),
            offset: offset.map(Into::into),
        }
    }
}

impl From<ast::SetExpr> for SetExprPlan {
    fn from(set_expr: ast::SetExpr) -> Self {
        match set_expr {
            ast::SetExpr::Select(select) => Self::Select(Box::new((*select).into())),
            ast::SetExpr::Values(values) => Self::Values(values.into()),
            ast::SetExpr::Union { left, right, all } => Self::Union {
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
                all,
            },
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
