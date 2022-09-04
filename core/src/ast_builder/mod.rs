#[cfg(feature = "alter-table")]
mod alter_table;
mod assignment;
mod column_def;
mod create_table;
mod data_type;
mod delete;
mod drop_table;
mod expr;
mod expr_list;
#[cfg(feature = "index")]
mod index;
mod order_by_expr;
mod query;
mod select;
mod select_item;
mod select_item_list;
mod show_columns;
mod table;
#[cfg(feature = "transaction")]
mod transaction;
mod update;

pub use {
    assignment::AssignmentNode,
    column_def::ColumnDefNode,
    create_table::CreateTableNode,
    data_type::DataTypeNode,
    delete::DeleteNode,
    drop_table::DropTableNode,
    expr_list::ExprList,
    order_by_expr::OrderByExprNode,
    query::QueryNode,
    select::{
        GroupByNode, HavingNode, LimitNode, LimitOffsetNode, OffsetLimitNode, OffsetNode,
        ProjectNode, SelectNode,
    },
    select_item::SelectItemNode,
    select_item_list::SelectItemList,
    show_columns::ShowColumnsNode,
    table::TableNode,
    update::UpdateNode,
};

/// Available expression builder functions
pub use expr::{col, exists, expr, nested, num, text, ExprNode};

#[cfg(feature = "alter-table")]
pub use alter_table::{
    AddColumnNode, AlterTableNode, DropColumnNode, RenameColumnNode, RenameTableNode,
};

#[cfg(feature = "index")]
pub use {index::CreateIndexNode, index::DropIndexNode};

/// Available aggregate or normal SQL functions
pub use expr::{
    aggregate::{avg, count, max, min, stdev, sum, variance, AggregateNode},
    function::{
        abs, acos, asin, atan, ceil, concat, cos, degrees, divide, exp, floor, gcd, generate_uuid,
        ifnull, lcm, left, ln, log, log10, log2, lpad, ltrim, modulo, now, pi, power, radians,
        repeat, reverse, right, round, rpad, rtrim, sign, sin, sqrt, substr, tan, upper,
        FunctionNode,
    },
};

/// Entry point function to build statement
pub fn table(table_name: &str) -> TableNode {
    let table_name = table_name.to_owned();

    TableNode { table_name }
}

/// Functions for building transaction statements
#[cfg(feature = "transaction")]
pub use transaction::{begin, commit, rollback};

#[cfg(test)]
fn test(actual: crate::result::Result<crate::ast::Statement>, expected: &str) {
    use crate::{parse_sql::parse, translate::translate};

    let parsed = &parse(expected).expect(expected)[0];
    let expected = translate(parsed);
    assert_eq!(actual, expected);
}

#[cfg(test)]
fn test_expr(actual: crate::ast_builder::ExprNode, expected: &str) {
    use crate::{parse_sql::parse_expr, translate::translate_expr};

    let parsed = &parse_expr(expected).expect(expected);
    let expected = translate_expr(parsed);
    assert_eq!(actual.try_into(), expected);
}

#[cfg(test)]
fn test_query(actual: crate::ast_builder::QueryNode, expected: &str) {
    use crate::{parse_sql::parse_query, translate::translate_query};

    let parsed = &parse_query(expected).expect(expected);
    let expected = translate_query(parsed);
    assert_eq!(actual.try_into(), expected);
}
