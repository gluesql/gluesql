mod filter;
mod group_by;
mod having;
mod join;
mod limit;
mod offset;
mod offset_limit;
mod order_by;
mod project;
mod root;
mod values;

use {
    super::Build,
    crate::{
        ast::{Query, Select, SetExpr},
        plan::{QueryPlan, SelectPlan, StatementPlan},
        result::Result,
    },
};
pub use {
    filter::FilterNode,
    group_by::GroupByNode,
    having::HavingNode,
    join::{HashJoinNode, JoinConstraintNode, JoinNode},
    limit::LimitNode,
    offset::OffsetNode,
    offset_limit::OffsetLimitNode,
    order_by::{SelectOrderByNode, ValuesOrderByNode},
    project::ProjectNode,
    root::{SelectNode, select},
    values::{ValuesNode, values},
};

pub(super) trait BuildSelectPlan {
    fn build_select_plan(self) -> Result<SelectPlan>;
}

pub(super) trait BuildSelect {
    fn build_select(self) -> Result<Select>;
}

pub(super) trait BuildQueryPlan {
    fn build_query_plan(self) -> Result<QueryPlan>;
}

pub(super) trait BuildQuery {
    fn build_query(self) -> Result<Query>;
}

impl<T: BuildSelectPlan> BuildQueryPlan for T {
    fn build_query_plan(self) -> Result<QueryPlan> {
        self.build_select_plan()
            .map(Box::new)
            .map(QueryPlan::Select)
    }
}

impl<T: BuildSelect> BuildQuery for T {
    fn build_query(self) -> Result<Query> {
        let select = self.build_select()?;
        let body = SetExpr::Select(Box::new(select));
        let query = Query {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };

        Ok(query)
    }
}

impl<T: BuildQueryPlan> Build for T {
    fn build(self) -> Result<StatementPlan> {
        let query = self.build_query_plan()?;

        Ok(StatementPlan::Query(query))
    }
}
