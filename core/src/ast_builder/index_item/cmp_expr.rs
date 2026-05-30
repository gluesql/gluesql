use {
    super::IndexItemNode,
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

    pub fn asc(self) -> IndexItemNode<'a> {
        IndexItemNode::NonClustered {
            name: self.index_name,
            asc: Some(true),
            cmp_expr: Some((self.operator, self.expr)),
        }
    }

    pub fn desc(self) -> IndexItemNode<'a> {
        IndexItemNode::NonClustered {
            name: self.index_name,
            asc: Some(false),
            cmp_expr: Some((self.operator, self.expr)),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        ast::{IndexOperator, Literal},
        ast_builder::{IndexItemNode, index_item::non_clustered},
        plan::{ExprPlan, IndexItemPlan},
    };

    #[test]
    fn test() {
        let actual = non_clustered("idx".to_owned())
            .eq("1")
            .asc()
            .build_index_item_plan()
            .unwrap();
        let expected = IndexItemPlan::NonClustered {
            name: "idx".to_owned(),
            asc: Some(true),
            cmp_expr: Some((
                IndexOperator::Eq,
                ExprPlan::Literal(Literal::Number(1.into())),
            )),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .eq("2")
            .desc()
            .build_index_item_plan()
            .unwrap();
        let expected = IndexItemPlan::NonClustered {
            name: "idx".to_owned(),
            asc: Some(false),
            cmp_expr: Some((
                IndexOperator::Eq,
                ExprPlan::Literal(Literal::Number(2.into())),
            )),
        };
        assert_eq!(actual, expected);

        let index_item: IndexItemNode = non_clustered("idx".to_owned()).eq("3").into();
        let actual = index_item.build_index_item_plan().unwrap();
        let expected = IndexItemPlan::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((
                IndexOperator::Eq,
                ExprPlan::Literal(Literal::Number(3.into())),
            )),
        };
        assert_eq!(actual, expected);
    }
}
