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
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Clone, Debug)]
pub struct ValuesData {
    pub values: Vec<Vec<Expr>>,
    pub order_by: Vec<Expr>,
    pub limit: Vec<Expr>,
    pub offset: Vec<Expr>,
}

#[derive(Clone, Debug)]
pub enum NodeData {
    Select(SelectData),
    Values(ValuesData),
}

impl NodeData {
    pub fn build_query(self) -> Query {
        match self {
            NodeData::Select(SelectData {
                projection,
                relation,
                group_by,
                having,
                order_by,
                offset,
                limit,
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

                Query {
                    body: SetExpr::Select(Box::new(select)),
                    order_by,
                    offset,
                    limit,
                }
            }
            NodeData::Values(values_data) => todo!(),
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
