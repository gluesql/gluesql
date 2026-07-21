use {
    super::{BuildQuery, BuildQueryPlan},
    crate::{
        ast::Query,
        plan::{LimitInputPlan, LimitPlan, OffsetPlan, QueryPlan},
        query_builder::{ExprNode, OffsetNode, QueryNode, TableFactorNode},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub(super) enum PrevNode<'a> {
    Offset(OffsetNode<'a>),
}

impl PrevNode<'_> {
    fn build_offset_plan(self) -> Result<OffsetPlan> {
        match self {
            Self::Offset(node) => node.build_offset_plan(),
        }
    }
}

impl BuildQuery for PrevNode<'_> {
    fn build_query(self) -> Result<Query> {
        match self {
            Self::Offset(node) => node.build_query(),
        }
    }
}

impl<'a> From<OffsetNode<'a>> for PrevNode<'a> {
    fn from(node: OffsetNode<'a>) -> Self {
        PrevNode::Offset(node)
    }
}

#[derive(Clone, Debug)]
pub struct OffsetLimitNode<'a> {
    prev_node: PrevNode<'a>,
    expr: ExprNode<'a>,
}

impl<'a> OffsetLimitNode<'a> {
    pub(super) fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode<'a> {
        QueryNode::OffsetLimitNode(self).alias_as(table_alias)
    }
}

impl BuildQueryPlan for OffsetLimitNode<'_> {
    fn build_query_plan(self) -> Result<QueryPlan> {
        let count = self.expr.build_expr_plan()?;
        self.prev_node.build_offset_plan().map(|offset| {
            QueryPlan::Limit(LimitPlan {
                input: LimitInputPlan::Offset(offset),
                count,
            })
        })
    }
}

impl BuildQuery for OffsetLimitNode<'_> {
    fn build_query(self) -> Result<Query> {
        let mut node_data = self.prev_node.build_query()?;
        node_data.limit = Some(self.expr.build_expr()?);

        Ok(node_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::query_builder::{table, test_query_builder};

    #[test]
    fn offset_limit() {
        // offset node -> limit node -> build node
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3);
        let expected = "
            SELECT * FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            OFFSET 1
            LIMIT 3;
        ";
        test_query_builder(actual, expected);

        // project node -> offset node -> limit node
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .project("city")
            .offset(1)
            .limit(3);
        let expected = "
            SELECT city FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            OFFSET 1
            LIMIT 3;
        ";
        test_query_builder(actual, expected);

        // select -> offset -> limit -> derived subquery
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3)
            .alias_as("Sub")
            .select();
        let expected = "
            SELECT * FROM (
                SELECT * FROM Bar
                GROUP BY city
                HAVING COUNT(name) < 100
                OFFSET 1
                LIMIT 3
            ) Sub
        ";
        test_query_builder(actual, expected);
    }
}
