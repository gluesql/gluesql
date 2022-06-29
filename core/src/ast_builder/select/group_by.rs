use crate::{
    ast::{Query, Select, SetExpr, Statement},
    ast_builder::{ExprList, ExprNode, HavingNode, LimitNode, OffsetNode, SelectNode},
    result::Result,
};

#[derive(Clone)]
pub enum PrevNode {
    Select(SelectNode),
}

impl PrevNode {
    fn build_select(self) -> Result<Select> {
        match self {
            Self::Select(node) => node.build_select(),
        }
    }
}

impl From<SelectNode> for PrevNode {
    fn from(node: SelectNode) -> Self {
        PrevNode::Select(node)
    }
}

#[derive(Clone)]
pub struct GroupByNode {
    prev_node: PrevNode,
    expr_list: ExprList,
}

impl GroupByNode {
    pub fn group_by<N: Into<PrevNode>, T: Into<ExprList>>(prev_node: N, expr_list: T) -> Self {
        Self {
            prev_node: prev_node.into(),
            expr_list: expr_list.into(),
        }
    }

    pub fn having<T: Into<ExprNode>>(self, expr: T) -> HavingNode {
        HavingNode::having(self, expr)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::offset(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::limit(self, expr)
    }

    pub fn build_select(self) -> Result<Select> {
        let mut select = self.prev_node.build_select()?;
        select.group_by = self.expr_list.try_into()?;

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
    fn group_by() {
        let actual = Builder::table("Bar")
            .select()
            .filter("id IS NULL")
            .group_by("id, (a + name)")
            .build();
        let expected = stmt(
            "
            SELECT * FROM Bar
            WHERE id IS NULL
            GROUP BY id, (a + name)
        ",
        );
        assert_eq!(actual, expected);
    }
}
