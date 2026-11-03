//! Old schema queries: wrap Projection, restore hash predicates, refuse index plans.

use {
    super::{SchemaDecodeError, expr::Expr},
    gluesql_core::ast::{self, BinaryOperator, Dictionary, IndexOperator, TableAlias},
    serde::Deserialize,
};

#[derive(Deserialize)]
pub(super) struct Query {
    body: SetExpr,
    order_by: Vec<OrderByExpr>,
    limit: Option<Expr>,
    offset: Option<Expr>,
}

impl Query {
    pub(super) fn into_current(self) -> Result<ast::Query, SchemaDecodeError> {
        Ok(ast::Query {
            body: self.body.into_current()?,
            order_by: self
                .order_by
                .into_iter()
                .map(OrderByExpr::into_current)
                .collect::<Result<_, _>>()?,
            limit: self.limit.map(Expr::into_current).transpose()?,
            offset: self.offset.map(Expr::into_current).transpose()?,
        })
    }
}

#[derive(Deserialize)]
struct Select {
    distinct: bool,
    projection: Projection,
    from: TableWithJoins,
    selection: Option<Expr>,
    group_by: Vec<Expr>,
    having: Option<Expr>,
}

impl Select {
    fn into_current(self) -> Result<ast::Select, SchemaDecodeError> {
        Ok(ast::Select {
            distinct: self.distinct,
            projection: self.projection.into_current()?,
            from: self.from.into_current()?,
            selection: self.selection.map(Expr::into_current).transpose()?,
            group_by: self
                .group_by
                .into_iter()
                .map(Expr::into_current)
                .collect::<Result<_, _>>()?,
            having: self.having.map(Expr::into_current).transpose()?,
        })
    }
}

#[derive(Deserialize)]
struct TableWithJoins {
    relation: TableFactor,
    joins: Vec<Join>,
}

impl TableWithJoins {
    fn into_current(self) -> Result<ast::TableWithJoins, SchemaDecodeError> {
        Ok(ast::TableWithJoins {
            relation: self.relation.into_current()?,
            joins: self
                .joins
                .into_iter()
                .map(Join::into_current)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Deserialize)]
struct OrderByExpr {
    expr: Expr,
    asc: Option<bool>,
}

impl OrderByExpr {
    fn into_current(self) -> Result<ast::OrderByExpr, SchemaDecodeError> {
        Ok(ast::OrderByExpr {
            expr: self.expr.into_current()?,
            asc: self.asc,
        })
    }
}

#[derive(Deserialize)]
enum SetExpr {
    Select(Box<Select>),
    Values(Values),
}

impl SetExpr {
    fn into_current(self) -> Result<ast::SetExpr, SchemaDecodeError> {
        Ok(match self {
            Self::Select(select) => ast::SetExpr::Select(Box::new(select.into_current()?)),
            Self::Values(values) => ast::SetExpr::Values(values.into_current()?),
        })
    }
}

#[derive(Deserialize)]
enum SelectItem {
    Expr { expr: Expr, label: String },
    QualifiedWildcard(String),
    Wildcard,
}

impl SelectItem {
    fn into_current(self) -> Result<ast::SelectItem, SchemaDecodeError> {
        Ok(match self {
            Self::Expr { expr, label } => ast::SelectItem::Expr {
                expr: expr.into_current()?,
                label,
            },
            Self::QualifiedWildcard(alias) => ast::SelectItem::QualifiedWildcard(alias),
            Self::Wildcard => ast::SelectItem::Wildcard,
        })
    }
}

#[derive(Deserialize)]
enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
}

impl JoinOperator {
    fn into_current(self) -> Result<ast::JoinOperator, SchemaDecodeError> {
        Ok(match self {
            Self::Inner(constraint) => ast::JoinOperator::Inner(constraint.into_current()?),
            Self::LeftOuter(constraint) => ast::JoinOperator::LeftOuter(constraint.into_current()?),
        })
    }
}

#[derive(Deserialize)]
enum JoinConstraint {
    On(Expr),
    None,
}

impl JoinConstraint {
    fn into_current(self) -> Result<ast::JoinConstraint, SchemaDecodeError> {
        Ok(match self {
            Self::On(expr) => ast::JoinConstraint::On(expr.into_current()?),
            Self::None => ast::JoinConstraint::None,
        })
    }
}

#[derive(Deserialize)]
struct Values(Vec<Vec<Expr>>);

impl Values {
    fn into_current(self) -> Result<ast::Values, SchemaDecodeError> {
        Ok(ast::Values(
            self.0
                .into_iter()
                .map(|row| row.into_iter().map(Expr::into_current).collect())
                .collect::<Result<_, _>>()?,
        ))
    }
}

#[derive(Deserialize)]
struct Projection(Vec<SelectItem>);

impl Projection {
    fn into_current(self) -> Result<ast::Projection, SchemaDecodeError> {
        Ok(ast::Projection::SelectItems(
            self.0
                .into_iter()
                .map(SelectItem::into_current)
                .collect::<Result<_, _>>()?,
        ))
    }
}

#[derive(Deserialize)]
enum TableFactor {
    Table {
        name: String,
        alias: Option<TableAlias>,
        index: Option<IndexItem>,
    },
    Derived {
        subquery: Query,
        alias: TableAlias,
    },
    Series {
        alias: TableAlias,
        size: Expr,
    },
    Dictionary {
        dict: Dictionary,
        alias: TableAlias,
    },
}

impl TableFactor {
    fn into_current(self) -> Result<ast::TableFactor, SchemaDecodeError> {
        Ok(match self {
            Self::Table { name, alias, index } => {
                // This runs only after the entire schema has decoded. A partially
                // parsed candidate must not prevent selection of another layout.
                if index.is_some() {
                    return Err(SchemaDecodeError::RecoveryRequired(
                        "historical index access plan requires explicit recovery",
                    ));
                }
                ast::TableFactor::Table { name, alias }
            }
            Self::Derived { subquery, alias } => ast::TableFactor::Derived {
                subquery: subquery.into_current()?,
                alias,
            },
            Self::Series { alias, size } => ast::TableFactor::Series {
                alias,
                size: size.into_current()?,
            },
            Self::Dictionary { dict, alias } => ast::TableFactor::Dictionary { dict, alias },
        })
    }
}

#[derive(Deserialize)]
#[expect(
    dead_code,
    reason = "Parse every index-plan field to validate the entire candidate before refusing recovery"
)]
enum IndexItem {
    PrimaryKey(Expr),
    NonClustered {
        name: String,
        asc: Option<bool>,
        cmp_expr: Option<(IndexOperator, Expr)>,
    },
}

#[derive(Deserialize)]
struct Join {
    relation: TableFactor,
    join_operator: JoinOperator,
    join_executor: JoinExecutor,
}

impl Join {
    fn into_current(self) -> Result<ast::Join, SchemaDecodeError> {
        let relation = self.relation.into_current()?;
        let join_operator = self.join_operator.into_current()?;
        let join_operator = match self.join_executor {
            JoinExecutor::NestedLoop => join_operator,
            JoinExecutor::Hash {
                key_expr,
                value_expr,
                where_clause,
            } => {
                let equality = ast::Expr::BinaryOp {
                    left: Box::new(key_expr.into_current()?),
                    op: BinaryOperator::Eq,
                    right: Box::new(value_expr.into_current()?),
                };
                let predicate = match where_clause.map(Expr::into_current).transpose()? {
                    Some(expr) => and(equality, expr),
                    None => equality,
                };
                let merge = |constraint| match constraint {
                    ast::JoinConstraint::On(expr) => ast::JoinConstraint::On(and(expr, predicate)),
                    ast::JoinConstraint::None => ast::JoinConstraint::On(predicate),
                };
                match join_operator {
                    ast::JoinOperator::Inner(constraint) => {
                        ast::JoinOperator::Inner(merge(constraint))
                    }
                    ast::JoinOperator::LeftOuter(constraint) => {
                        ast::JoinOperator::LeftOuter(merge(constraint))
                    }
                }
            }
        };
        Ok(ast::Join {
            relation,
            join_operator,
        })
    }
}

#[derive(Deserialize)]
enum JoinExecutor {
    NestedLoop,
    Hash {
        key_expr: Expr,
        value_expr: Expr,
        where_clause: Option<Expr>,
    },
}

fn and(left: ast::Expr, right: ast::Expr) -> ast::Expr {
    ast::Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }
}

#[cfg(test)]
mod query_layout_changes {
    use {
        super::{
            Expr, IndexItem, Join, JoinConstraint, JoinExecutor, JoinOperator, Projection,
            SchemaDecodeError, TableFactor,
        },
        crate::migration::decode,
        gluesql_core::ast::{self, BinaryOperator, IndexOperator},
    };

    fn old_identifier(name: &str) -> Expr {
        Expr::Identifier(name.into())
    }

    fn identifier(name: &str) -> ast::Expr {
        ast::Expr::Identifier(name.into())
    }

    fn binary(left: ast::Expr, op: BinaryOperator, right: ast::Expr) -> ast::Expr {
        ast::Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn old_table() -> TableFactor {
        TableFactor::Table {
            name: "Source".into(),
            alias: None,
            index: None,
        }
    }

    fn current_table() -> ast::TableFactor {
        ast::TableFactor::Table {
            name: "Source".into(),
            alias: None,
        }
    }

    #[test]
    fn projection_vec_becomes_select_items() {
        // v1 Projection is a bare Vec: length 1, SelectItem::Wildcard tag 2.
        // The current Projection requires the SelectItems enum wrapper.
        let bytes = [1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(
            decode::deserialize::<Projection>(&bytes)
                .unwrap()
                .into_current()
                .unwrap(),
            ast::Projection::SelectItems(vec![ast::SelectItem::Wildcard])
        );
    }

    #[test]
    fn removed_hash_executor_keys_become_an_on_equality() {
        let join = Join {
            relation: old_table(),
            join_operator: JoinOperator::Inner(JoinConstraint::None),
            join_executor: JoinExecutor::Hash {
                key_expr: old_identifier("left_key"),
                value_expr: old_identifier("right_key"),
                where_clause: None,
            },
        };
        assert_eq!(
            join.into_current().unwrap(),
            ast::Join {
                relation: current_table(),
                join_operator: ast::JoinOperator::Inner(ast::JoinConstraint::On(binary(
                    identifier("left_key"),
                    BinaryOperator::Eq,
                    identifier("right_key"),
                ))),
            }
        );
    }

    #[test]
    fn hash_filter_and_existing_left_join_condition_are_both_restored() {
        let join = Join {
            relation: old_table(),
            join_operator: JoinOperator::LeftOuter(JoinConstraint::On(old_identifier(
                "original_on",
            ))),
            join_executor: JoinExecutor::Hash {
                key_expr: old_identifier("left_key"),
                value_expr: old_identifier("right_key"),
                where_clause: Some(old_identifier("remaining_filter")),
            },
        };
        // original_on AND ((left_key = right_key) AND remaining_filter)
        let expected_on = binary(
            identifier("original_on"),
            BinaryOperator::And,
            binary(
                binary(
                    identifier("left_key"),
                    BinaryOperator::Eq,
                    identifier("right_key"),
                ),
                BinaryOperator::And,
                identifier("remaining_filter"),
            ),
        );
        assert_eq!(
            join.into_current().unwrap(),
            ast::Join {
                relation: current_table(),
                join_operator: ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(expected_on)),
            }
        );
    }

    #[test]
    fn removing_nested_loop_executor_keeps_the_existing_condition() {
        let join = Join {
            relation: old_table(),
            join_operator: JoinOperator::Inner(JoinConstraint::On(old_identifier("original_on"))),
            join_executor: JoinExecutor::NestedLoop,
        };
        assert_eq!(
            join.into_current().unwrap(),
            ast::Join {
                relation: current_table(),
                join_operator: ast::JoinOperator::Inner(ast::JoinConstraint::On(identifier(
                    "original_on"
                ))),
            }
        );
    }

    #[test]
    fn removed_index_access_plans_require_explicit_recovery() {
        // Either index plan may contain filters/order that cannot be reconstructed.
        for index in [
            IndexItem::PrimaryKey(old_identifier("key")),
            IndexItem::NonClustered {
                name: "by_name".into(),
                asc: Some(false),
                cmp_expr: Some((IndexOperator::GtEq, old_identifier("predicate"))),
            },
        ] {
            let table = TableFactor::Table {
                name: "Source".into(),
                alias: None,
                index: Some(index),
            };
            assert!(matches!(
                table.into_current(),
                Err(SchemaDecodeError::RecoveryRequired(
                    "historical index access plan requires explicit recovery"
                ))
            ));
        }
    }
}
