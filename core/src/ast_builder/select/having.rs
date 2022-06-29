use crate::{
    ast::{Query, Select, SetExpr, Statement},
    ast_builder::{ExprNode, GroupByNode, LimitNode, OffsetNode, SelectNode},
    result::Result,
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
    GroupBy(GroupByNode),
}

impl PrevNode {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
            Self::GroupBy(node) => node.build_select(),
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

#[derive(Clone)]
pub struct HavingNode {
    prev_node: PrevNode,
    expr: ExprNode,
}

impl HavingNode {
    pub fn having<N: Into<PrevNode>, T: Into<ExprNode>>(prev_node: N, expr: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr: expr.into(),
        }
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::offset(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::limit(self, expr)
    }

    pub fn build_select(self) -> Result<Select> {
        let mut select = self.prev_node.build_select()?;
        select.having = Some(self.expr.try_into()?);

        Ok(select)
    }

    pub fn build_query(self) -> Result<Query> {
        let select = self.build_select()?;
        let query = Query {
            body: SetExpr::Select(Box::new(select)),
            offset: None,
            limit: None,
        };

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
    fn having() {
        let actual = Builder::table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .having("COUNT(id) > 10")
            .build();
        let expected = stmt(
            "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
            HAVING COUNT(id) > 10
        ",
        );
        assert_eq!(actual, expected);
    }
}
