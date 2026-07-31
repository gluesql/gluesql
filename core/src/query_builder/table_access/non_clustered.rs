use {
    super::IndexPredicateNode,
    crate::{ast::IndexOperator, query_builder::ExprNode},
};

#[derive(Clone, Debug)]
pub struct NonClusteredNode {
    pub index_name: String,
}

impl<'a> NonClusteredNode {
    pub fn gt<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexPredicateNode<'a> {
        IndexPredicateNode::new(self.index_name, IndexOperator::Gt, expr.into())
    }

    pub fn lt<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexPredicateNode<'a> {
        IndexPredicateNode::new(self.index_name, IndexOperator::Lt, expr.into())
    }

    pub fn gte<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexPredicateNode<'a> {
        IndexPredicateNode::new(self.index_name, IndexOperator::GtEq, expr.into())
    }

    pub fn lte<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexPredicateNode<'a> {
        IndexPredicateNode::new(self.index_name, IndexOperator::LtEq, expr.into())
    }

    pub fn eq<T: Into<ExprNode<'a>>>(self, expr: T) -> IndexPredicateNode<'a> {
        IndexPredicateNode::new(self.index_name, IndexOperator::Eq, expr.into())
    }
}

pub fn non_clustered(index_name: String) -> NonClusteredNode {
    NonClusteredNode { index_name }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{IndexOperator, Literal},
        plan::{ExprPlan, IndexPredicatePlan, TableAccessPlan},
        query_builder::{TableAccessNode, num, table_access::non_clustered},
        result::{Error, QueryBuilderError},
    };

    #[test]
    fn test() {
        let index_node: TableAccessNode = non_clustered("idx".to_owned()).gt("1").into();
        let actual = index_node.build_table_access_plan().unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Gt,
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            }),
        };
        assert_eq!(actual, expected);

        let index_node: TableAccessNode = non_clustered("idx".to_owned()).lt("1").into();
        let actual = index_node.build_table_access_plan().unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Lt,
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            }),
        };
        assert_eq!(actual, expected);

        let index_node: TableAccessNode = non_clustered("idx".to_owned()).gte("1").into();
        let actual = index_node.build_table_access_plan().unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::GtEq,
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            }),
        };
        assert_eq!(actual, expected);

        let index_node: TableAccessNode = non_clustered("idx".to_owned()).lte("1").into();
        let actual = index_node.build_table_access_plan().unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::LtEq,
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            }),
        };
        assert_eq!(actual, expected);

        let index_node: TableAccessNode = non_clustered("idx".to_owned()).eq("1").into();
        let actual = index_node.build_table_access_plan().unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: Some(IndexPredicatePlan {
                operator: IndexOperator::Eq,
                expr: ExprPlan::Literal(Literal::Number(1.into())),
            }),
        };
        assert_eq!(actual, expected);

        let index_node: TableAccessNode = non_clustered("idx".to_owned()).into();
        let actual = index_node.build_table_access_plan().unwrap();
        let expected = TableAccessPlan::Index {
            name: "idx".to_owned(),
            asc: None,
            predicate: None,
        };
        assert_eq!(actual, expected);

        let index_node: TableAccessNode = non_clustered("idx".to_owned()).eq(num(f64::NAN)).into();
        let actual = index_node.build_table_access_plan();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::FailedToParseNumeric(f64::NAN.to_string()),
        ));
        assert_eq!(actual, expected);
    }
}
