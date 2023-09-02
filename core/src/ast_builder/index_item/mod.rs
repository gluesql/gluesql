mod cmp_expr;
mod non_clustered;
mod order;
mod primary_key;
mod primary_key_expr;

pub use {
    crate::ast::IndexItem, crate::result::Result, cmp_expr::CmpExprNode,
    non_clustered::non_clustered, non_clustered::NonClusteredNode, order::OrderNode,
    primary_key::primary_key, primary_key::PrimaryKeyNode, primary_key_expr::PrimaryKeyCmpExprNode,
};

use {
    super::{insert::Expr, select::Prebuild, ExprNode},
    crate::ast::IndexOperator,
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

impl<'a> Prebuild<IndexItem> for IndexItemNode<'a> {
    fn prebuild(self) -> Result<IndexItem> {
        match self {
            IndexItemNode::NonClustered {
                name,
                asc,
                cmp_expr,
            } => build_non_clustered(name, asc, cmp_expr),
            IndexItemNode::PrimaryKey(expr) => build_primary_key(expr),
        }
    }
}

fn build_non_clustered(
    name: String,
    asc: Option<bool>,
    cmp_expr: Option<(IndexOperator, ExprNode)>,
) -> Result<IndexItem> {
    let (index_operator, expr) = cmp_expr.unzip();
    let expr_result: Option<Expr> = expr.map(ExprNode::try_into).transpose()?;
    let cmp_expr_result: Option<(IndexOperator, Expr)> = index_operator
        .zip(expr_result)
        .map(|(index_operator, expr)| (index_operator, expr));

    Ok(IndexItem::NonClustered {
        name,
        asc,
        cmp_expr: cmp_expr_result,
    })
}

fn build_primary_key(expr: ExprNode) -> Result<IndexItem> {
    Ok(IndexItem::PrimaryKey(expr.try_into()?))
}
