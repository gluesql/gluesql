mod group_by;
mod having;
mod limit;
mod limit_offset;
mod offset;
mod offset_limit;
mod order_by;
mod project;
mod root;

pub use {
    group_by::GroupByNode, having::HavingNode, limit::LimitNode, limit_offset::LimitOffsetNode,
    offset::OffsetNode, offset_limit::OffsetLimitNode, order_by::OrderByNode, project::ProjectNode,
    root::SelectNode,
};

use crate::{
    ast::{Expr, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableWithJoins},
    result::Result,
};

pub trait Prebuild {
    fn prebuild(self) -> Result<NodeData>;
}

#[derive(Clone)]
pub struct NodeData {
    pub projection: Vec<SelectItem>,
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

impl NodeData {
    pub fn build_query(self) -> Query {
        let NodeData {
            projection,
            from,
            selection,
            group_by,
            having,
            order_by,
            offset,
            limit,
        } = self;

        let select = Select {
            projection,
            from,
            selection,
            group_by,
            having,
        };

        Query {
            body: SetExpr::Select(Box::new(select)),
            order_by,
            offset,
            limit,
        }
    }
    fn build_stmt(self) -> Statement {
        let query = self.build_query();

        Statement::Query(query)
    }
}
