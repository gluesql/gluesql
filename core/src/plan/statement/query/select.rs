use {
    crate::{
        ast,
        plan::{AggregatePlan, ExprPlan, OrderByExprPlan, ProjectionPlan, TableWithJoinsPlan},
    },
    serde::{Deserialize, Serialize},
};

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
pub struct SelectOrderByPlan {
    pub input: Box<SelectPlan>,
    pub exprs: Vec<OrderByExprPlan>,
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
