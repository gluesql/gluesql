use {
    super::TableAccessNode,
    crate::{ast::IndexOperator, query_builder::ExprNode},
};

#[derive(Clone, Debug)]
pub struct IndexPredicateNode<'a> {
    pub index_name: String,
    pub operator: IndexOperator,
    pub expr: ExprNode<'a>,
}

impl<'a> IndexPredicateNode<'a> {
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

    pub fn asc(self) -> TableAccessNode<'a> {
        let Self {
            index_name,
            operator,
            expr,
        } = self;

        TableAccessNode::Index {
            name: index_name,
            asc: Some(true),
            predicate: Some((operator, expr)),
        }
    }

    pub fn desc(self) -> TableAccessNode<'a> {
        let Self {
            index_name,
            operator,
            expr,
        } = self;

        TableAccessNode::Index {
            name: index_name,
            asc: Some(false),
            predicate: Some((operator, expr)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{IndexOperator, Literal},
        plan::{ExprPlan, IndexPredicatePlan, TableAccessPlan},
        query_builder::{TableAccessNode, table_access::non_clustered},
    };

    #[test]
    fn test() {
        let actual = non_clustered("idx".to_owned())
            .eq("1")
            .asc()
            .build_table_access_plan()
            .unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: Some(true),
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            }),
        };
        assert_eq!(actual, expected);

        let actual = non_clustered("idx".to_owned())
            .eq("2")
            .desc()
            .build_table_access_plan()
            .unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: Some(false),
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: ExprPlan::Literal(Literal::Number(2.into())),
            }),
        };
        assert_eq!(actual, expected);

        let index_item: TableAccessNode = non_clustered("idx".to_owned()).eq("3").into();
        let actual = index_item.build_table_access_plan().unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: ExprPlan::Literal(Literal::Number(3.into())),
            }),
        };
        assert_eq!(actual, expected);
    }
}
