mod delete;
mod expr;
mod expr_list;
mod select;
mod select_item;
mod select_item_list;
mod table;

pub use {
    delete::DeleteNode,
    expr_list::ExprList,
    select::{
        GroupByNode, HavingNode, LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode,
        ProjectNode, SelectNode,
    },
    select_item::SelectItemNode,
    select_item_list::SelectItemList,
    table::TableNode,
};

/// Available expression builder functions
pub use expr::{col, expr, nested, num, text, ExprNode};

/// Available aggregate or normal SQL functions
pub use expr::{
    aggregate::{avg, max, min, sum, variance, AggregateNode},
    function::{
        abs, acos, asin, atan, ceil, cos, floor, ifnull, left, log10, log2, pi, reverse, right,
        sin, tan, upper, FunctionNode,
    },
};

/// Entry point function to build statement
pub fn table(table_name: &str) -> TableNode {
    let table_name = table_name.to_owned();

    TableNode { table_name }
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
