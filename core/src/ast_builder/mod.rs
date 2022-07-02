mod delete;
mod expr;
mod expr_list;
mod select;
mod select_item_list;

pub use {
    delete::DeleteNode,
    expr::{abs, col, max, nested, num, sum, text, ExprNode, FunctionNode},
    expr_list::ExprList,
    select::{
        GroupByNode, HavingNode, LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode,
        ProjectNode, SelectNode,
    },
    select_item_list::SelectItemList,
};

pub fn table(table_name: &str) -> TableNode {
    let table_name = table_name.to_owned();

    TableNode { table_name }
}

pub struct TableNode {
    table_name: String,
}

impl TableNode {
    pub fn select(self) -> SelectNode {
        SelectNode::new(self.table_name)
    }

    pub fn delete(self) -> DeleteNode {
        DeleteNode::new(self.table_name)
    }
}

#[cfg(test)]
fn test(actual: crate::result::Result<crate::ast::Statement>, expected: &str) {
    use crate::{parse_sql::parse, translate::translate};

    let parsed = &parse(expected).unwrap()[0];
    let expected = translate(parsed);
    assert_eq!(actual, expected);
}

#[cfg(test)]
fn test_expr(actual: crate::ast_builder::ExprNode, expected: &str) {
    use crate::{parse_sql::parse_expr, translate::translate_expr};

    let parsed = &parse_expr(expected).unwrap();
    let expected = translate_expr(parsed);
    assert_eq!(actual.try_into(), expected);
}
