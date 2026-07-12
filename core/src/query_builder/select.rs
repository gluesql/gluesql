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
        plan::{QueryBodyPlan, QueryPlan, SelectPlan, SetExprPlan, StatementPlan},
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
    order_by::OrderByNode,
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

pub(super) trait BuildQueryBodyPlan {
    fn build_query_body_plan(self) -> Result<QueryBodyPlan>;
}

pub(super) trait BuildQuery {
    fn build_query(self) -> Result<Query>;
}

impl<T: BuildSelectPlan> BuildQueryBodyPlan for T {
    fn build_query_body_plan(self) -> Result<QueryBodyPlan> {
        let select = self.build_select_plan()?;
        let body = SetExprPlan::Select(Box::new(select));

        Ok(QueryBodyPlan {
            body,
            order_by: Vec::new(),
        })
    }
}

impl<T: BuildQueryBodyPlan> BuildQueryPlan for T {
    fn build_query_plan(self) -> Result<QueryPlan> {
        self.build_query_body_plan().map(QueryPlan::Body)
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
