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
        ast::{Query, Select, SetExpr, Statement},
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
    root::{select, SelectNode},
    values::{values, ValuesNode},
};

pub trait Prebuild<T> {
    fn prebuild(self) -> Result<T>;
}

impl<T: Prebuild<Select>> Prebuild<Query> for T {
    fn prebuild(self) -> Result<Query> {
        let select = self.prebuild()?;
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

impl<T: Prebuild<Query>> Build for T {
    fn build(self) -> Result<Statement> {
        let query = self.prebuild()?;

        Ok(Statement::Query(query))
    }
}
