use {
    super::{IndexItemNode, OrderNode},
    crate::{ast::IndexOperator, ast_builder::ExprNode},
};

#[derive(Clone, Debug)]
pub struct CmpExprNode<'a> {
    pub index_name: String,
    pub operator: IndexOperator,
    pub expr: ExprNode<'a>,
}

impl<'a> CmpExprNode<'a> {
    pub fn new<T: Into<ExprNode<'a>>>(
        index_name: String,
        operator: IndexOperator,
        expr: T,
    ) -> Self {
        Self {
            index_name,
            operator,
            expr: expr.into(),
        }
    }

    pub fn asc(self) -> OrderNode<'a> {
        OrderNode::new(self, true)
    }

    pub fn desc(self) -> OrderNode<'a> {
        OrderNode::new(self, false)
    }

    pub fn build(self) -> IndexItemNode<'a> {
        IndexItemNode::NonClustered {
            name: self.index_name,
            asc: None,
            cmp_expr: Some((self.operator, self.expr)),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        ast::IndexOperator,
        ast_builder::{
            index_item::{non_clustered, IndexItem},
            select::Prebuild,
            to_expr,
        },
    };

    #[test]
    fn test() {
        let actual = non_clustered("idx".to_owned())
            .eq("1")
            .asc()
            .build()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: Some(true),
            cmp_expr: Some((IndexOperator::Eq, to_expr("1"))),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .eq("2")
            .desc()
            .build()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: Some(false),
            cmp_expr: Some((IndexOperator::Eq, to_expr("2"))),
        };
        assert_eq!(actual, expected);
    }
}
