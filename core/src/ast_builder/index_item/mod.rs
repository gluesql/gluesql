mod cmp_expr;
mod non_clustered;
mod primary_key;

use {
    super::{select::Prebuild, ExprNode},
    crate::ast::{Expr, IndexOperator},
};
pub use {
    crate::{ast::IndexItem, result::Result},
    cmp_expr::CmpExprNode,
    non_clustered::{non_clustered, NonClusteredNode},
    primary_key::{primary_key, PrimaryKeyNode},
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

impl<'a> From<NonClusteredNode> for IndexItemNode<'a> {
    fn from(non_clustered: NonClusteredNode) -> Self {
        IndexItemNode::NonClustered {
            name: non_clustered.index_name,
            asc: None,
            cmp_expr: None,
        }
    }
}

impl<'a> Prebuild<IndexItem> for IndexItemNode<'a> {
    fn prebuild(self) -> Result<IndexItem> {
        match self {
            IndexItemNode::NonClustered {
                name,
                asc,
                cmp_expr,
            } => {
                let (index_operator, expr) = cmp_expr.unzip();
                let expr_result: Option<Expr> = expr.map(ExprNode::try_into).transpose()?;
                let cmp_expr_result: Option<(IndexOperator, Expr)> =
                    index_operator.zip(expr_result);

                Ok(IndexItem::NonClustered {
                    name,
                    asc,
                    cmp_expr: cmp_expr_result,
                })
            }
            IndexItemNode::PrimaryKey(expr) => Ok(IndexItem::PrimaryKey(expr.try_into()?)),
        }
    }
}
