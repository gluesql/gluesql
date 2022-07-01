use {
    super::{build_stmt, NodeData, Prebuild},
    crate::{
        ast::{Expr, ObjectName, SelectItem, Statement, TableFactor, TableWithJoins},
        ast_builder::{ExprList, ExprNode, GroupByNode, LimitNode, OffsetNode},
        result::Result,
    },
};

#[derive(Clone)]
pub struct SelectNode {
    table_name: String,
    columns: Vec<String>,
    filter_expr: Option<ExprNode>,
}

impl SelectNode {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            columns: vec![],
            filter_expr: None,
        }
    }

    pub fn col(mut self, column: &str) -> Self {
        self.columns.push(column.to_owned());

        self
    }

    pub fn filter<T: Into<ExprNode>>(mut self, expr: T) -> Self {
        self.filter_expr = Some(expr.into());

        self
    }

    pub fn group_by<T: Into<ExprList>>(self, expr_list: T) -> GroupByNode {
        GroupByNode::group_by(self, expr_list)
    }

    pub fn offset<T: Into<ExprNode>>(self, expr: T) -> OffsetNode {
        OffsetNode::offset(self, expr)
    }

    pub fn limit<T: Into<ExprNode>>(self, expr: T) -> LimitNode {
        LimitNode::limit(self, expr)
    }

    pub fn build(self) -> Result<Statement> {
        let select_data = self.prebuild()?;

        Ok(build_stmt(select_data))
    }
}

impl Prebuild for SelectNode {
    fn prebuild(self) -> Result<NodeData> {
        let relation = TableFactor::Table {
            name: ObjectName(vec![self.table_name]),
            alias: None,
            index: None,
        };

        let from = TableWithJoins {
            relation,
            joins: vec![],
        };

        let selection = self.filter_expr.map(Expr::try_from).transpose()?;

        Ok(NodeData {
            projection: vec![SelectItem::Wildcard],
            from,
            selection,
            group_by: vec![],
            having: None,
            offset: None,
            limit: None,
        })
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
    fn select() {
        let actual = Builder::table("App").select().build();
        let expected = stmt("SELECT * FROM App");
        assert_eq!(actual, expected);

        let actual = Builder::table("Bar").select().filter("id IS NULL").build();
        let expected = stmt("SELECT * FROM Bar WHERE id IS NULL");
        assert_eq!(actual, expected);

        let actual = Builder::table("Foo")
            .select()
            .filter("id > name")
            .offset(3)
            .limit(100)
            .build();
        let expected = stmt(
            "
            SELECT * FROM Foo
            WHERE id > name
            OFFSET 3
            LIMIT 100
        ",
        );
        assert_eq!(actual, expected);
    }
}
