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
        ast::{AstLiteral, Expr, IndexOperator},
        ast_builder::{
            index_item::{non_clustered, IndexItem},
            select::Prebuild,
            IndexItemNode,
        },
    };

    #[test]
    fn test() {
        let actual = non_clustered("idx".to_owned())
            .eq("1")
            .asc()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: Some(true),
            cmp_expr: Some((
                IndexOperator::Eq,
                Expr::Literal(AstLiteral::Number(1.into())),
            )),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .eq("2")
            .desc()
            .prebuild()
            .unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: Some(false),
            cmp_expr: Some((
                IndexOperator::Eq,
                Expr::Literal(AstLiteral::Number(2.into())),
            )),
        };
        assert_eq!(actual, expected);

        let index_item: IndexItemNode = non_clustered("idx".to_owned()).eq("3").into();
        let actual = index_item.prebuild().unwrap();
        let expected = IndexItem::NonClustered {
            name: "idx".to_owned(),
            asc: None,
            cmp_expr: Some((
                IndexOperator::Eq,
                Expr::Literal(AstLiteral::Number(3.into())),
            )),
        };
        assert_eq!(actual, expected);
    }
}
