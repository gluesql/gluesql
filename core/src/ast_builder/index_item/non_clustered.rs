use {
    super::CmpExprNode,
    crate::{ast::IndexOperator, ast_builder::ExprNode},
};

#[derive(Clone, Debug)]
pub struct NonClusteredNode {
    pub index_name: String,
}

impl<'a> NonClusteredNode {
    pub fn gt<T: Into<ExprNode<'a>>>(self, expr: T) -> CmpExprNode<'a> {
        CmpExprNode::new(self.index_name, IndexOperator::Gt, expr.into())
    }

    pub fn lt<T: Into<ExprNode<'a>>>(self, expr: T) -> CmpExprNode<'a> {
        CmpExprNode::new(self.index_name, IndexOperator::Lt, expr.into())
    }

    pub fn gte<T: Into<ExprNode<'a>>>(self, expr: T) -> CmpExprNode<'a> {
        CmpExprNode::new(self.index_name, IndexOperator::GtEq, expr.into())
    }

    pub fn lte<T: Into<ExprNode<'a>>>(self, expr: T) -> CmpExprNode<'a> {
        CmpExprNode::new(self.index_name, IndexOperator::LtEq, expr.into())
    }

    pub fn eq<T: Into<ExprNode<'a>>>(self, expr: T) -> CmpExprNode<'a> {
        CmpExprNode::new(self.index_name, IndexOperator::Eq, expr.into())
    }
}

pub fn non_clustered(index_name: String) -> NonClusteredNode {
    NonClusteredNode { index_name }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{AstLiteral, Expr, IndexOperator},
        ast_builder::{
            index_item::{non_clustered, IndexItem},
            select::Prebuild,
            IndexItemNode,
        },
    };

    #[test]
    fn test() {
        let index_node: IndexItemNode = non_clustered("idx".to_owned()).gt("1").into();
        let actual = index_node.prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((
                IndexOperator::Gt,
                Expr::Literal(AstLiteral::Number(1.into())),
            )),
        };
        assert_eq!(actual, expected);

        let index_node: IndexItemNode = non_clustered("idx".to_owned()).lt("1").into();
        let actual = index_node.prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((
                IndexOperator::Lt,
                Expr::Literal(AstLiteral::Number(1.into())),
            )),
        };
        assert_eq!(actual, expected);

        let index_node: IndexItemNode = non_clustered("idx".to_owned()).gte("1").into();
        let actual = index_node.prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((
                IndexOperator::GtEq,
                Expr::Literal(AstLiteral::Number(1.into())),
            )),
        };
        assert_eq!(actual, expected);

        let index_node: IndexItemNode = non_clustered("idx".to_owned()).lte("1").into();
        let actual = index_node.prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((
                IndexOperator::LtEq,
                Expr::Literal(AstLiteral::Number(1.into())),
            )),
        };
        assert_eq!(actual, expected);

        let index_node: IndexItemNode = non_clustered("idx".to_owned()).eq("1").into();
        let actual = index_node.prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((
                IndexOperator::Eq,
                Expr::Literal(AstLiteral::Number(1.into())),
            )),
        };
        assert_eq!(actual, expected);

        let index_node: IndexItemNode = non_clustered("idx".to_owned()).into();
        let actual = index_node.prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: None,
        };
        assert_eq!(actual, expected);
    }
}
