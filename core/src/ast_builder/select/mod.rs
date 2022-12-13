use crate::ast::Values;

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

#[derive(Clone, Debug)]
pub struct SelectData {
    pub projection: Vec<SelectItem>,
    pub relation: TableFactor,
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    /// WHERE
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Clone, Debug)]
pub enum QueryData {
    Select(SelectData),
    Values(Vec<Vec<Expr>>),
}

#[derive(Clone, Debug)]
pub struct NodeData {
    pub body: QueryData,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

impl NodeData {
    pub fn build_query(self) -> Query {
        let NodeData {
            body,
            order_by,
            limit,
            offset,
        } = self;
        let body = match body {
            QueryData::Select(SelectData {
                projection,
                relation,
                group_by,
                having,
                joins,
                filter,
            }) => {
                let selection = filter.map(Expr::try_from).and_then(|expr| expr.ok());
                let from = TableWithJoins { relation, joins };

                let select = Select {
                    projection,
                    from,
                    selection,
                    group_by,
                    having,
                };

                SetExpr::Select(Box::new(select))
            }
            QueryData::Values(values) => SetExpr::Values(Values(values)),
        };

        Query {
            body,
            order_by,
            limit,
            offset,
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
