mod alter_table;
mod assignment;
mod build;
mod column_def;
mod column_list;
mod create_table;
mod data_type;
mod delete;
mod drop_table;
mod error;
mod execute;
mod expr;
mod expr_list;
mod index;
mod insert;
mod order_by_expr;
mod order_by_expr_list;
mod query;
mod select;
mod select_item;
mod select_item_list;
mod show_columns;
mod table_factor;
mod table_name;
mod transaction;
mod update;

pub use {
    assignment::AssignmentNode,
    build::Build,
    column_def::ColumnDefNode,
    column_list::ColumnList,
    create_table::CreateTableNode,
    data_type::DataTypeNode,
    delete::DeleteNode,
    drop_table::DropTableNode,
    error::AstBuilderError,
    execute::Execute,
    expr_list::ExprList,
    insert::InsertNode,
    order_by_expr::OrderByExprNode,
    order_by_expr_list::OrderByExprList,
    query::QueryNode,
    select::{
        values, FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode,
        LimitNode, OffsetLimitNode, OffsetNode, OrderByNode, ProjectNode, SelectNode,
    },
    select_item::SelectItemNode,
    select_item_list::SelectItemList,
    show_columns::ShowColumnsNode,
    table_factor::{
        glue_indexes, glue_objects, glue_table_columns, glue_tables, series, TableFactorNode,
    },
    table_name::table,
    update::UpdateNode,
};

/// Available expression builder functions
pub use expr::{
    case, col, date, exists, expr, factorial, minus, nested, not, not_exists, null, num,
    numeric::NumericNode, plus, subquery, text, time, timestamp, ExprNode,
};

pub use alter_table::{
    AddColumnNode, AlterTableNode, DropColumnNode, RenameColumnNode, RenameTableNode,
};

pub use {index::CreateIndexNode, index::DropIndexNode};

/// Available aggregate or normal SQL functions
pub use expr::{
    aggregate::{avg, count, max, min, stdev, sum, variance, AggregateNode},
    function::{
        abs, acos, asin, atan, cast, ceil, concat, concat_ws, cos, degrees, divide, exp, extract,
        floor, format, gcd, generate_uuid, ifnull, lcm, left, ln, log, log10, log2, lower, lpad,
        ltrim, modulo, now, pi, position, power, radians, rand, repeat, reverse, right, round,
        rpad, rtrim, sign, sin, sqrt, substr, tan, to_date, to_time, to_timestamp, upper,
        FunctionNode,
    },
};

/// Functions for building transaction statements
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
