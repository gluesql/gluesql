mod index_predicate;
mod non_clustered;
mod primary_key;

use {
    super::ExprNode,
    crate::{
        ast::IndexOperator,
        plan::{IndexPredicatePlan, TableAccessPlan},
    },
};
pub use {
    crate::result::Result,
    index_predicate::IndexPredicateNode,
    non_clustered::{NonClusteredNode, non_clustered},
    primary_key::{PrimaryKeyNode, primary_key},
};

#[derive(Clone, Debug)]
pub enum TableAccessNode<'a> {
    FullScan,
    Index {
        name: String,
        asc: Option<bool>,
        predicate: Option<(IndexOperator, ExprNode<'a>)>,
    },
    PrimaryKey(ExprNode<'a>),
}

impl<'a> From<IndexPredicateNode<'a>> for TableAccessNode<'a> {
    fn from(predicate: IndexPredicateNode<'a>) -> Self {
        let IndexPredicateNode {
            index_name,
            operator,
            expr,
        } = predicate;

        TableAccessNode::Index {
            name: index_name,
            asc: None,
            predicate: Some((operator, expr)),
        }
    }
}

impl From<NonClusteredNode> for TableAccessNode<'_> {
    fn from(non_clustered: NonClusteredNode) -> Self {
        TableAccessNode::Index {
            name: non_clustered.index_name,
            asc: None,
            predicate: None,
        }
    }
}

impl TableAccessNode<'_> {
    pub(super) fn build_table_access_plan(self) -> Result<TableAccessPlan> {
        match self {
            Self::FullScan => Ok(TableAccessPlan::FullScan),
            Self::Index {
                name,
                asc,
                predicate,
            } => Ok(TableAccessPlan::Index {
                name,
                asc,
                predicate: predicate
                    .map(|(operator, expr)| {
                        expr.build_expr_plan()
                            .map(|expr| IndexPredicatePlan { operator, expr })
                    })
                    .transpose()?,
            }),
            Self::PrimaryKey(expr) => Ok(TableAccessPlan::PrimaryKey {
                expr: expr.build_expr_plan()?,
            }),
        }
    }
}
