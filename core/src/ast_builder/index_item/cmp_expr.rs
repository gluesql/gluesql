use {
    super::OrderNode,
    crate::{
        ast::{IndexItem, IndexOperator},
        ast_builder::insert::Expr,
        ast_builder::ExprNode,
    },
};

#[derive(Clone, Debug)]
pub struct CmpExprNode {
    pub index_name: String,
    pub operator: IndexOperator,
    pub expr: Expr,
}

impl CmpExprNode {
    pub fn new(index_name: String, operator: IndexOperator, expr: ExprNode) -> Self {
        Self {
            index_name,
            operator,
            expr: expr.try_into().ok().unwrap(),
        }
    }

    pub fn asc(self) -> OrderNode {
        OrderNode::new(self, true)
    }

    pub fn desc(self) -> OrderNode {
        OrderNode::new(self, false)
    }

    pub fn build(self) -> IndexItem {
        IndexItem::NonClustered {
            name: self.index_name,
            asc: None,
            cmp_expr: Some((self.operator, self.expr)),
        }
    }
}

#[cfg(test)]
mod tests {

    use {
        super::IndexItem,
        crate::{
            ast::IndexOperator,
            ast_builder::{index_item::non_clustered, to_expr},
        },
    };

    #[test]
    fn test() {
        let actual = non_clustered("idx".to_owned()).eq("1").asc().build();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: Some(true),
            cmp_expr: Some((IndexOperator::Eq, to_expr("1"))),
        };

        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned()).eq("1").desc().build();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: Some(false),
            cmp_expr: Some((IndexOperator::Eq, to_expr("1"))),
        };

        assert_eq!(actual, expected);
    }
}
