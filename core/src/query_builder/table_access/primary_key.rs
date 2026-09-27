use {super::TableAccessNode, crate::query_builder::ExprNode};

#[derive(Clone, Debug)]
pub struct PrimaryKeyNode;

impl<'a> PrimaryKeyNode {
    pub fn eq<T: Into<ExprNode<'a>>>(self, expr: T) -> TableAccessNode<'a> {
        TableAccessNode::PrimaryKey(expr.into())
    }
}

/// Entry point function to Primary Key
pub fn primary_key() -> PrimaryKeyNode {
    PrimaryKeyNode
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::Literal,
        plan::{ExprPlan, TableAccessPlan},
        query_builder::{num, primary_key},
        result::{Error, QueryBuilderError},
    };

    #[test]
    fn test() {
        let actual = primary_key().eq("1").build_table_access_plan().unwrap();
        let expected = TableAccessPlan::PrimaryKey {
            expr: ExprPlan::Literal(Literal::Number(1.into())),
        };
        assert_eq!(actual, expected);

        let actual = primary_key().eq(num(f64::NAN)).build_table_access_plan();
        let expected = Err(Error::QueryBuilder(
            QueryBuilderError::FailedToParseNumeric(f64::NAN.to_string()),
        ));
        assert_eq!(actual, expected);
    }
}
