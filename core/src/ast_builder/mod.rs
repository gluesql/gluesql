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
mod expr_with_alias;
mod index;
mod index_item;
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

/// Available expression builder functions
pub use expr::{
    ExprNode, bitwise_not, bytea, case, col, date, exists, expr, factorial, minus, nested, not,
    not_exists, null, num, numeric::NumericNode, plus, subquery, text, time, timestamp, uuid,
};
/// Available aggregate or normal SQL functions
pub use expr::{
    aggregate::{AggregateNode, avg, count, max, min, stdev, sum, variance},
    function,
};
/// Functions for building transaction statements
pub use transaction::{begin, commit, rollback};
pub use {
    alter_table::{
        AddColumnNode, AlterTableNode, DropColumnNode, RenameColumnNode, RenameTableNode,
    },
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
    expr_with_alias::ExprWithAliasNode,
    index::{CreateIndexNode, DropIndexNode},
    index_item::{
        CmpExprNode, IndexItemNode, NonClusteredNode, PrimaryKeyNode, non_clustered, primary_key,
    },
    insert::InsertNode,
    order_by_expr::OrderByExprNode,
    order_by_expr_list::OrderByExprList,
    query::QueryNode,
    select::{
        FilterNode, GroupByNode, HashJoinNode, HavingNode, JoinConstraintNode, JoinNode, LimitNode,
        OffsetLimitNode, OffsetNode, OrderByNode, ProjectNode, SelectNode, select, values,
    },
    select_item::SelectItemNode,
    select_item_list::SelectItemList,
    show_columns::ShowColumnsNode,
    table_factor::{
        TableFactorNode, glue_indexes, glue_objects, glue_table_columns, glue_tables, series,
    },
    table_name::table,
    update::UpdateNode,
};

#[cfg(test)]
fn test(actual: crate::result::Result<crate::ast::Statement>, expected: &str) {
    use crate::{parse_sql::parse, translate::translate};

    let parsed = &parse(expected).expect(expected)[0];
    let expected = translate(parsed);
    pretty_assertions::assert_eq!(actual, expected);
}

#[cfg(test)]
fn test_expr(actual: crate::ast_builder::ExprNode, expected: &str) {
    use crate::{parse_sql::parse_expr, translate::translate_expr};

    let parsed = &parse_expr(expected).expect(expected);
    let expected = translate_expr(parsed);
    pretty_assertions::assert_eq!(actual.try_into(), expected);
}

#[cfg(test)]
fn test_query(actual: crate::ast_builder::QueryNode, expected: &str) {
    use crate::{parse_sql::parse_query, translate::translate_query};

    let parsed = &parse_query(expected).expect(expected);
    let expected = translate_query(parsed);
    pretty_assertions::assert_eq!(actual.try_into(), expected);
}
