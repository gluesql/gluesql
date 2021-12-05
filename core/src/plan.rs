use {
    crate::{
        ast::{
            AstLiteral, BinaryOperator, Expr, IndexItem, IndexOperator, OrderByExpr, Query, Select,
            SetExpr, Statement, TableFactor, TableWithJoins,
        },
        data::{get_name, Schema, SchemaIndex, SchemaIndexOrd},
        result::Result,
        store::Store,
    },
    async_recursion::async_recursion,
    gluesql_utils::Vector,
    std::fmt::Debug,
};

pub async fn plan<T: Debug>(storage: &dyn Store<T>, statement: Statement) -> Result<Statement> {
    match statement {
        Statement::Query(query) => plan_query(storage, *query)
            .await
            .map(Box::new)
            .map(Statement::Query),
        _ => Ok(statement),
    }
}

struct Indexes(Vec<SchemaIndex>);

impl Indexes {
    fn find(&self, target: &Expr) -> Option<String> {
        self.0
            .iter()
            .find(|SchemaIndex { expr, .. }| expr == target)
            .map(|SchemaIndex { name, .. }| name.to_owned())
    }

    fn find_ordered(&self, target: &OrderByExpr) -> Option<String> {
        self.0
            .iter()
            .find(|SchemaIndex { expr, order, .. }| {
                if expr != &target.expr {
                    return false;
                }

                matches!(
                    (target.asc, order),
                    (_, SchemaIndexOrd::Both)
                        | (Some(true), SchemaIndexOrd::Asc)
                        | (None, SchemaIndexOrd::Asc)
                        | (Some(false), SchemaIndexOrd::Desc)
                )
            })
            .map(|SchemaIndex { name, .. }| name.to_owned())
    }
}

async fn plan_query<T: Debug>(storage: &dyn Store<T>, query: Query) -> Result<Query> {
    let Query {
        body,
        order_by,
        limit,
        offset,
    } = query;

    let select = match body {
        SetExpr::Select(select) => select,
        SetExpr::Values(_) => {
            return Ok(Query {
                body,
                order_by,
                limit,
                offset,
            });
        }
    };

    let TableWithJoins { relation, .. } = &select.from;
    let table_name = match relation {
        TableFactor::Table { name, .. } => name,
    };
    let table_name = get_name(table_name)?;
    let indexes = match storage.fetch_schema(table_name).await? {
        Some(Schema { indexes, .. }) => Indexes(indexes),
        None => {
            return Ok(Query {
                body: SetExpr::Select(select),
                order_by,
                limit,
                offset,
            });
        }
    };

    let index = order_by.last().and_then(|value_expr| {
        indexes.find_ordered(value_expr).map(|name| IndexItem {
            name,
            asc: value_expr.asc,
            cmp_expr: None,
        })
    });

    match index {
        index if index.is_some() => {
            let Select {
                projection,
                from,
                selection,
                group_by,
                having,
            } = *select;

            let TableWithJoins { relation, joins } = from;
            let (name, alias) = match relation {
                TableFactor::Table { name, alias, .. } => (name, alias),
            };

            let from = TableWithJoins {
                relation: TableFactor::Table { name, alias, index },
                joins,
            };

            let select = Select {
                projection,
                from,
                selection,
                group_by,
                having,
            };

            Ok(Query {
                body: SetExpr::Select(Box::new(select)),
                order_by: Vector::from(order_by).pop().0.into(),
                limit,
                offset,
            })
        }
        _ => {
            let select = plan_select(storage, &indexes, *select).await?;
            let body = SetExpr::Select(Box::new(select));
            let query = Query {
                body,
                order_by,
                limit,
                offset,
            };

            Ok(query)
        }
    }
}

async fn plan_select<T: Debug>(
    storage: &dyn Store<T>,
    indexes: &Indexes,
    select: Select,
) -> Result<Select> {
    let Select {
        projection,
        from,
        selection,
        group_by,
        having,
    } = select;

    let selection = match selection {
        Some(expr) => expr,
        None => {
            return Ok(Select {
                projection,
                from,
                selection,
                group_by,
                having,
            });
        }
    };

    match plan_index(storage, indexes, selection).await? {
        Planned::Expr(selection) => Ok(Select {
            projection,
            from,
            selection: Some(selection),
            group_by,
            having,
        }),
        Planned::IndexedExpr {
            index_name,
            index_op,
            index_value_expr,
            selection,
        } => {
            let TableWithJoins { relation, joins } = from;
            let (name, alias) = match relation {
                TableFactor::Table { name, alias, .. } => (name, alias),
            };

            let index = Some(IndexItem {
                name: index_name,
                asc: None,
                cmp_expr: Some((index_op, index_value_expr)),
            });
            let from = TableWithJoins {
                relation: TableFactor::Table { name, alias, index },
                joins,
            };

            Ok(Select {
                projection,
                from,
                selection,
                group_by,
                having,
            })
        }
    }
}

enum Planned {
    IndexedExpr {
        index_name: String,
        index_op: IndexOperator,
        index_value_expr: Expr,
        selection: Option<Expr>,
    },
    Expr(Expr),
}

#[async_recursion(?Send)]
async fn plan_index<T: Debug>(
    storage: &dyn Store<T>,
    indexes: &Indexes,
    selection: Expr,
) -> Result<Planned> {
    match selection {
        Expr::Nested(expr) => plan_index(storage, indexes, *expr).await,
        Expr::IsNull(expr) => Ok(search_is_null(indexes, true, expr)),
        Expr::IsNotNull(expr) => Ok(search_is_null(indexes, false, expr)),
        Expr::Subquery(query) => plan_query(storage, *query)
            .await
            .map(Box::new)
            .map(Expr::Subquery)
            .map(Planned::Expr),
        Expr::Exists(query) => plan_query(storage, *query)
            .await
            .map(Box::new)
            .map(Expr::Exists)
            .map(Planned::Expr),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => plan_query(storage, *subquery)
            .await
            .map(Box::new)
            .map(|subquery| Expr::InSubquery {
                expr,
                subquery,
                negated,
            })
            .map(Planned::Expr),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left = match plan_index(storage, indexes, *left).await? {
                Planned::Expr(selection) => selection,
                Planned::IndexedExpr {
                    index_name,
                    index_value_expr,
                    index_op,
                    selection,
                } => {
                    let selection = match selection {
                        Some(expr) => Expr::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::And,
                            right,
                        },
                        None => *right,
                    };

                    return Ok(Planned::IndexedExpr {
                        index_name,
                        index_op,
                        index_value_expr,
                        selection: Some(selection),
                    });
                }
            };

            match plan_index(storage, indexes, *right).await? {
                Planned::Expr(expr) => Ok(Planned::Expr(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(expr),
                })),
                Planned::IndexedExpr {
                    index_name,
                    index_op,
                    index_value_expr,
                    selection,
                } => {
                    let selection = match selection {
                        Some(expr) => Expr::BinaryOp {
                            left: Box::new(left),
                            op: BinaryOperator::And,
                            right: Box::new(expr),
                        },
                        None => left,
                    };

                    Ok(Planned::IndexedExpr {
                        index_name,
                        index_value_expr,
                        index_op,
                        selection: Some(selection),
                    })
                }
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Gt,
            right,
        } => Ok(search_index_op(indexes, IndexOperator::Gt, left, right)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Lt,
            right,
        } => Ok(search_index_op(indexes, IndexOperator::Lt, left, right)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::GtEq,
            right,
        } => Ok(search_index_op(indexes, IndexOperator::GtEq, left, right)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::LtEq,
            right,
        } => Ok(search_index_op(indexes, IndexOperator::LtEq, left, right)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => Ok(search_index_op(indexes, IndexOperator::Eq, left, right)),
        _ => Ok(Planned::Expr(selection)),
    }
}

fn search_is_null(indexes: &Indexes, null: bool, expr: Box<Expr>) -> Planned {
    match indexes.find(expr.as_ref()) {
        Some(index_name) => {
            let index_op = if null {
                IndexOperator::Eq
            } else {
                IndexOperator::Lt
            };

            Planned::IndexedExpr {
                index_name,
                index_op,
                index_value_expr: Expr::Literal(AstLiteral::Null),
                selection: None,
            }
        }
        None => {
            let expr = if null {
                Expr::IsNull(expr)
            } else {
                Expr::IsNotNull(expr)
            };

            Planned::Expr(expr)
        }
    }
}

fn search_index_op(
    indexes: &Indexes,
    index_op: IndexOperator,
    left: Box<Expr>,
    right: Box<Expr>,
) -> Planned {
    if let Some(index_name) = indexes
        .find(left.as_ref())
        .and_then(|index_name| is_stateless(right.as_ref()).then(|| index_name))
    {
        Planned::IndexedExpr {
            index_name,
            index_op,
            index_value_expr: *right,
            selection: None,
        }
    } else if let Some(index_name) = indexes
        .find(right.as_ref())
        .and_then(|index_name| is_stateless(left.as_ref()).then(|| index_name))
    {
        Planned::IndexedExpr {
            index_name,
            index_op: index_op.reverse(),
            index_value_expr: *left,
            selection: None,
        }
    } else if let Expr::Nested(left) = *left {
        search_index_op(indexes, index_op, left, right)
    } else if let Expr::Nested(right) = *right {
        search_index_op(indexes, index_op, left, right)
    } else {
        Planned::Expr(Expr::BinaryOp {
            left,
            op: index_op.into(),
            right,
        })
    }
}

fn is_stateless(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(AstLiteral::Null) => false,
        Expr::Literal(_) => true,
        Expr::TypedString { .. } => true,
        Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr) => is_stateless(expr.as_ref()),
        Expr::BinaryOp { left, right, .. } => {
            is_stateless(left.as_ref()) && is_stateless(right.as_ref())
        }
        _ => false,
    }
}
