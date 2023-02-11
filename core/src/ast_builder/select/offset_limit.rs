use {
    super::Prebuild,
    crate::{
        ast::Query,
        ast_builder::{ExprNode, OffsetNode, QueryNode, TableFactorNode},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub enum PrevNode<'a> {
    Offset(OffsetNode<'a>),
}

impl<'a> Prebuild<Query> for PrevNode<'a> {
    fn prebuild(self) -> Result<Query> {
        match self {
            Self::Offset(node) => node.prebuild(),
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
    pub fn new<N: Into<PrevNode<'a>>, T: Into<ExprNode<'a>>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn alias_as(self, table_alias: &'a str) -> TableFactorNode {
        QueryNode::OffsetLimitNode(self).alias_as(table_alias)
    }
}

impl<'a> Prebuild<Query> for OffsetLimitNode<'a> {
    fn prebuild(self) -> Result<Query> {
        let mut node_data = self.prev_node.prebuild()?;
        node_data.limit = Some(self.expr.try_into()?);

        Ok(node_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{table, test, Build};

    #[test]
    fn offset_limit() {
        // offset node -> limit node -> build node
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3)
            .build();
        let expected = "
            SELECT * FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            OFFSET 1
            LIMIT 3;
        ";
        test(actual, expected);

        // project node -> offset node -> limit node
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .project("city")
            .offset(1)
            .limit(3)
            .build();
        let expected = "
            SELECT city FROM Bar
            GROUP BY city
            HAVING COUNT(name) < 100
            OFFSET 1
            LIMIT 3;
        ";
        test(actual, expected);

        // select -> offset -> limit -> derived subquery
        let actual = table("Bar")
            .select()
            .group_by("city")
            .having("COUNT(name) < 100")
            .offset(1)
            .limit(3)
            .alias_as("Sub")
            .select()
            .build();
        let expected = "
            SELECT * FROM (
                SELECT * FROM Bar
                GROUP BY city
                HAVING COUNT(name) < 100
                OFFSET 1
                LIMIT 3
            ) Sub
        ";
        test(actual, expected);
    }
}
