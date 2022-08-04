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

trait Prebuild {
    fn prebuild(self) -> Result<NodeData>;
}

#[derive(Clone)]
struct NodeData {
    pub projection: Vec<SelectItem>,
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

impl NodeData {
    fn build_stmt(self) -> Statement {
        let NodeData {
            projection,
            from,
            selection,
            group_by,
            having,
            offset,
            limit,
            order_by,
        } = self;

        let select = Select {
            projection,
            from,
            selection,
            group_by,
            having,
            order_by,
        };

        let query = Query {
            body: SetExpr::Select(Box::new(select)),
            offset,
            limit,
        };

        Statement::Query(Box::new(query))
    }
}
