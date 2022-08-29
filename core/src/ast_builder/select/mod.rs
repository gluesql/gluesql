mod filter;
mod group_by;
mod having;
mod join;
mod join_constraint;
mod limit;
mod limit_offset;
mod offset;
mod offset_limit;
mod project;
mod root;

pub use {
    filter::FilterNode, group_by::GroupByNode, having::HavingNode, join::JoinNode,
    join_constraint::JoinConstraintNode, limit::LimitNode, limit_offset::LimitOffsetNode,
    offset::OffsetNode, offset_limit::OffsetLimitNode, project::ProjectNode, root::SelectNode,
};

use crate::{
    ast::{Expr, Join, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins},
    result::Result,
};

pub trait Prebuild {
    fn prebuild(self) -> Result<NodeData>;
}

#[derive(Clone)]
pub struct NodeData {
    pub projection: Vec<SelectItem>,
    pub relation: TableFactor,
    pub joins: Vec<Join>,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

impl NodeData {
    pub fn build_query(self) -> Query {
        let NodeData {
            projection,
            relation,
            selection,
            group_by,
            having,
            offset,
            limit,
            joins,
        } = self;

        let from = TableWithJoins { relation, joins };

        let select = Select {
            projection,
            from,
            selection,
            group_by,
            having,
            order_by: vec![],
        };

        Query {
            body: SetExpr::Select(Box::new(select)),
            offset,
            limit,
        }
    }
    fn build_stmt(self) -> Statement {
        let query = self.build_query();

        Statement::Query(query)
    }
}
