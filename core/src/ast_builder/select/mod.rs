mod filter;
mod group_by;
mod having;
mod join;
mod limit;
mod limit_offset;
mod offset;
mod offset_limit;
mod order_by;
mod project;
mod root;

pub use {
    filter::FilterNode,
    group_by::GroupByNode,
    having::HavingNode,
    join::{HashJoinNode, JoinConstraintNode, JoinNode},
    limit::LimitNode,
    limit_offset::LimitOffsetNode,
    offset::OffsetNode,
    offset_limit::OffsetLimitNode,
    order_by::OrderByNode,
    project::ProjectNode,
    root::SelectNode,
};

use {
    super::Build,
    crate::{
        ast::{
            Expr, Join, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
            TableWithJoins,
        },
        result::Result,
    },
};

pub trait Prebuild {
    fn prebuild(self) -> Result<NodeData>;
}

#[derive(Clone)]
pub struct NodeData {
    pub projection: Vec<SelectItem>,
    pub relation: TableFactor,
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    /// WHERE
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
            relation,
            group_by,
            having,
            order_by,
            offset,
            limit,
            joins,
            filter,
        } = self;

        let selection = filter.map(Expr::try_from).and_then(|expr| expr.ok());
        let from = TableWithJoins { relation, joins };

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

impl<T> Build for T
where
    T: Prebuild,
{
    fn build(self) -> Result<Statement> {
        self.prebuild().map(NodeData::build_stmt)
    }
}
