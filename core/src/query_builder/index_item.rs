mod cmp_expr;
mod non_clustered;
mod primary_key;

use {
    super::ExprNode,
    crate::{
        ast::IndexOperator,
        plan::{ExprPlan, IndexItemPlan},
    },
};
pub use {
    crate::result::Result,
    cmp_expr::CmpExprNode,
    non_clustered::{NonClusteredNode, non_clustered},
    primary_key::{PrimaryKeyNode, primary_key},
};

#[derive(Clone, Debug)]
pub enum IndexItemNode<'a> {
    NonClustered {
        name: String,
        asc: Option<bool>,
        cmp_expr: Option<(IndexOperator, ExprNode<'a>)>,
    },
    PrimaryKey(ExprNode<'a>),
}

impl<'a> From<CmpExprNode<'a>> for IndexItemNode<'a> {
    fn from(cmp_expr: CmpExprNode<'a>) -> Self {
        IndexItemNode::NonClustered {
            name: cmp_expr.index_name,
            asc: None,
            cmp_expr: Some((cmp_expr.operator, cmp_expr.expr)),
        }
    }
}

impl From<NonClusteredNode> for IndexItemNode<'_> {
    fn from(non_clustered: NonClusteredNode) -> Self {
        IndexItemNode::NonClustered {
            name: non_clustered.index_name,
            asc: None,
            cmp_expr: None,
        }
    }
}

impl IndexItemNode<'_> {
    pub(super) fn build_index_item_plan(self) -> Result<IndexItemPlan> {
        match self {
            IndexItemNode::NonClustered {
                name,
                asc,
                cmp_expr,
            } => {
                let (index_operator, expr) = cmp_expr.unzip();
                let expr_result: Option<ExprPlan> =
                    expr.map(ExprNode::build_expr_plan).transpose()?;
                let cmp_expr_result: Option<(IndexOperator, ExprPlan)> =
                    index_operator.zip(expr_result);

                Ok(IndexItemPlan::NonClustered {
                    name,
                    asc,
                    cmp_expr: cmp_expr_result,
                })
            }
            IndexItemNode::PrimaryKey(expr) => {
                Ok(IndexItemPlan::PrimaryKey(expr.build_expr_plan()?))
            }
        }
    }
}
