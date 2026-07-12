use {
    crate::{
        ast,
        plan::{AggregatePlan, ExprPlan, ProjectionPlan, TableWithJoinsPlan},
    },
    serde::{Deserialize, Serialize},
};

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
pub struct ValuesPlan(pub Vec<Vec<ExprPlan>>);

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
