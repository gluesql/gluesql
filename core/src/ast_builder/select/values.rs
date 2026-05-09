use crate::{
    ast::{Query, SetExpr, Values},
    ast_builder::{
        ExprList, ExprNode, LimitNode, OffsetNode, OrderByExprList, OrderByNode, QueryNode,
        TableFactorNode,
        select::{BuildQuery, BuildQueryPlan},
    },
    plan::{QueryPlan, SetExprPlan, ValuesPlan},
    result::Result,
};

#[derive(Clone, Debug)]
pub struct ValuesNode<'a> {
    pub values: Vec<ExprList<'a>>,
}

impl<'a> ValuesNode<'a> {
    pub fn order_by<T: Into<OrderByExprList<'a>>>(self, order_by_exprs: T) -> OrderByNode<'a> {
        OrderByNode::new(self, order_by_exprs)
    }

    pub fn offset<T: Into<ExprNode<'a>>>(self, expr: T) -> OffsetNode<'a> {
        OffsetNode::new(self, expr)
    }

    pub fn limit<T: Into<ExprNode<'a>>>(self, expr: T) -> LimitNode<'a> {
        LimitNode::new(self, expr)
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::ValuesNode(self).alias_as(table_alias)
    }
}

impl BuildQueryPlan for ValuesNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        let values = self
            .values
            .into_iter()
            .map(ExprList::build_exprs_plan)
            .collect::<Result<Vec<_>>>()?;

        let body = SetExprPlan::Values(ValuesPlan(values));

        Ok(QueryPlan {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
    }
}

impl BuildQuery for ValuesNode<'_> {
    fn build_query(self) -> Result<Query> {
        let values = self
            .values
            .into_iter()
            .map(ExprList::build_exprs)
            .collect::<Result<Vec<_>>>()?;

        let body = SetExpr::Values(Values(values));

        Ok(Query {
            body,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
    }
}

pub fn values<'a, T: Into<ExprList<'a>>>(values: Vec<T>) -> ValuesNode<'a> {
    let values: Vec<ExprList> = values.into_iter().map(Into::into).collect();

    ValuesNode { values }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{num, test_query_builder};

    #[test]
    fn values() {
        use crate::ast_builder::values;

        let actual = values(vec![vec![num(7)]]);
        let expected = "VALUES(7)";
        test_query_builder(actual, expected);

        let actual = values(vec!["1, 'a'", "2, 'b'"]);
        let expected = "VALUES(1, 'a'), (2, 'b')";
        test_query_builder(actual, expected);

        let actual = values(vec!["1, 'a'", "2, 'b'"]).order_by(vec!["column1 desc"]);
        let expected = "VALUES(1, 'a'), (2, 'b') ORDER BY column1 desc";
        test_query_builder(actual, expected);

        let actual = values(vec!["1, 'a'", "2, 'b'"]).offset(1);
        let expected = "VALUES(1, 'a'), (2, 'b') offset 1";
        test_query_builder(actual, expected);

        let actual = values(vec!["1, 'a'", "2, 'b'"]).limit(1);
        let expected = "VALUES(1, 'a'), (2, 'b') limit 1";
        test_query_builder(actual, expected);

        let actual = values(vec!["1, 'a'", "2, 'b'"]).offset(1).limit(1);
        let expected = "VALUES(1, 'a'), (2, 'b') offset 1 limit 1";
        test_query_builder(actual, expected);

        let actual = values(vec!["1, 'a'", "2, 'b'"]).alias_as("Sub").select();
        let expected = "SELECT * FROM (VALUES(1, 'a'), (2, 'b')) AS Sub";
        test_query_builder(actual, expected);
    }
}
