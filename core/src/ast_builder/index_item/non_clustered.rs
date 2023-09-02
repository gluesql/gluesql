use {
    super::{CmpExprNode, IndexItemNode},
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

    pub fn build(self) -> IndexItemNode<'a> {
        IndexItemNode::NonClustered {
            name: self.index_name,
            asc: None,
            cmp_expr: None,
        }
    }
}

pub fn non_clustered(index_name: String) -> NonClusteredNode {
    NonClusteredNode { index_name }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::IndexOperator,
        ast_builder::index_item::IndexItem,
        ast_builder::{index_item::non_clustered::non_clustered, select::Prebuild, to_expr},
    };

    #[test]
    fn test() {
        let actual = non_clustered("idx".to_owned())
            .gt("1")
            .build()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((IndexOperator::Gt, to_expr("1"))),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .lt("1")
            .build()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((IndexOperator::Lt, to_expr("1"))),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .gte("1")
            .build()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((IndexOperator::GtEq, to_expr("1"))),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .lte("1")
            .build()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((IndexOperator::LtEq, to_expr("1"))),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .eq("1")
            .build()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((IndexOperator::Eq, to_expr("1"))),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned()).build().prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: None,
        };
        assert_eq!(actual, expected);
    }
}
