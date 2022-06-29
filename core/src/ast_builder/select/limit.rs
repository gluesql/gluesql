use crate::{
    ast::{Query, Statement},
    ast_builder::{ExprNode, GroupByNode, HavingNode, LimitOffsetNode, SelectNode},
    result::Result,
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
    Having(HavingNode),
}

impl PrevNode {
    fn build_query(self) -> Result<Query> {
        match self {
            Self::Select(node) => node.build_query(),
            Self::GroupBy(node) => node.build_query(),
            Self::Having(node) => node.build_query(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

impl From<GroupByNode> for PrevNode {
    fn from(node: GroupByNode) -> Self {
        PrevNode::GroupBy(node)
    }
}

impl From<HavingNode> for PrevNode {
    fn from(node: HavingNode) -> Self {
        PrevNode::Having(node)
    }
}

#[derive(Clone)]
pub struct LimitNode {
    prev_node: PrevNode,
    expr: ExprNode,
}

impl LimitNode {
    pub fn limit<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> LimitOffsetNode {
        LimitOffsetNode::offset(self, expr)
    }

    pub fn build_query(self) -> Result<Query> {
        let mut query = self.prev_node.build_query()?;
        query.limit = Some(self.expr.try_into()?);

        Ok(query)
    }

    pub fn build(self) -> Result<Statement> {
        let query = self.build_query()?;

        Ok(Statement::Query(Box::new(query)))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::Statement, ast_builder::Builder, parse_sql::parse, result::Result,
        translate::translate,
    };

    fn stmt(sql: &str) -> Result<Statement> {
        let parsed = &parse(sql).unwrap()[0];

        translate(parsed)
    }

    #[test]
    fn offset() {
        let actual = Builder::table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .limit(100)
            .offset(5)
            .build();
        let expected = stmt(
            "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
            LIMIT 100
            OFFSET 5
        ",
        );
        assert_eq!(actual, expected);
    }
}
