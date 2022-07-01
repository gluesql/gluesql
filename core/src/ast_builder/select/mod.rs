mod group_by;
mod having;
mod limit;
mod limit_offset;
mod offset;
mod offset_limit;
mod project;
mod root;

pub use {
    group_by::GroupByNode, having::HavingNode, limit::LimitNode, limit_offset::LimitOffsetNode,
    offset::OffsetNode, offset_limit::OffsetLimitNode, project::ProjectNode, root::SelectNode,
};

use crate::{
    ast::{Expr, Query, Select, SelectItem, SetExpr, Statement, TableWithJoins},
    result::Result,
};

struct NodeData {
    pub projection: Vec<SelectItem>,
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

trait Prebuild {
    fn prebuild(self) -> Result<NodeData>;
}

fn build_stmt(select_data: NodeData) -> Statement {
    let NodeData {
        projection,
        from,
        selection,
        group_by,
        having,
        offset,
        limit,
    } = select_data;

    let select = Select {
        projection,
        from,
        selection,
        group_by,
        having,
        order_by: vec![],
    };

    let query = Query {
        body: SetExpr::Select(Box::new(select)),
        offset,
        limit,
    };

    Statement::Query(Box::new(query))
}
