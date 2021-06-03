use {
    crate::{
        ast::{
            AstLiteral, BinaryOperator, Expr, IndexItem, IndexOperator, Query, Select, SetExpr,
            Statement, TableFactor, TableWithJoins,
        },
        data::{get_name, Schema},
        result::Result,
        store::Store,
    },
    boolinator::Boolinator,
    std::fmt::Debug,
};

pub async fn plan<T: 'static + Debug>(
    storage: &dyn Store<T>,
    statement: Statement,
) -> Result<Statement> {
    match statement {
        Statement::Query(query) => plan_query(storage, *query)
            .await
            .map(Box::new)
            .map(Statement::Query),
        _ => Ok(statement),
    }
}

async fn plan_query<T: 'static + Debug>(storage: &dyn Store<T>, query: Query) -> Result<Query> {
    let Query {
        body,
        limit,
        offset,
    } = query;

    let select = match body {
        SetExpr::Select(select) => plan_select(storage, *select).await.map(Box::new)?,
        SetExpr::Values(_) => {
            return Ok(Query {
                body,
                limit,
                offset,
            });
        }
    };

    let body = SetExpr::Select(select);
    let query = Query {
        body,
        limit,
        offset,
    };

    Ok(query)
}

struct Indexes(Vec<(String, Expr)>);

impl Indexes {
    fn find(&self, target: &Expr) -> Option<String> {
        self.0
            .iter()
            .find(|(_, expr)| expr == target)
            .map(|(index_name, _)| index_name.to_owned())
    }
}

async fn plan_select<T: 'static + Debug>(storage: &dyn Store<T>, select: Select) -> Result<Select> {
    let Select {
        projection,
        from,
        selection,
        group_by,
        having,
    } = select;

    let TableWithJoins { relation, .. } = &from;

    let table_name = match relation {
        TableFactor::Table { name, .. } => name,
    };
    let table_name = get_name(table_name)?;

    let indexes = match storage.fetch_schema(table_name).await? {
        Some(Schema { indexes, .. }) => Indexes(indexes),
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

    match search_index(&indexes, selection) {
        Searched::NotFound(selection) => Ok(Select {
            projection,
            from,
            selection: Some(selection),
            group_by,
            having,
        }),
        Searched::Found {
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
                op: index_op,
                value_expr: index_value_expr,
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

enum Searched {
    Found {
        index_name: String,
        index_op: IndexOperator,
        index_value_expr: Expr,
        selection: Option<Expr>,
    },
    NotFound(Expr),
}

fn search_index(indexes: &Indexes, selection: Expr) -> Searched {
    match selection {
        Expr::Nested(expr) => search_index(indexes, *expr),
        Expr::IsNull(expr) => search_is_null(indexes, true, expr),
        Expr::IsNotNull(expr) => search_is_null(indexes, false, expr),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left = match search_index(indexes, *left) {
                Searched::NotFound(selection) => selection,
                Searched::Found {
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

                    return Searched::Found {
                        index_name,
                        index_op,
                        index_value_expr,
                        selection: Some(selection),
                    };
                }
            };

            match search_index(indexes, *right) {
                Searched::NotFound(expr) => Searched::NotFound(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(expr),
                }),
                Searched::Found {
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

                    Searched::Found {
                        index_name,
                        index_value_expr,
                        index_op,
                        selection: Some(selection),
                    }
                }
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Gt,
            right,
        } => search_index_op(indexes, IndexOperator::Gt, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Lt,
            right,
        } => search_index_op(indexes, IndexOperator::Lt, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::GtEq,
            right,
        } => search_index_op(indexes, IndexOperator::GtEq, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::LtEq,
            right,
        } => search_index_op(indexes, IndexOperator::LtEq, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => search_index_op(indexes, IndexOperator::Eq, left, right),
        _ => Searched::NotFound(selection),
    }
}

fn search_is_null(indexes: &Indexes, null: bool, expr: Box<Expr>) -> Searched {
    match indexes.find(expr.as_ref()) {
        Some(index_name) => {
            let index_op = if null {
                IndexOperator::Eq
            } else {
                IndexOperator::Lt
            };

            Searched::Found {
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

            Searched::NotFound(expr)
        }
    }
}

fn search_index_op(
    indexes: &Indexes,
    index_op: IndexOperator,
    left: Box<Expr>,
    right: Box<Expr>,
) -> Searched {
    if let Some(index_name) = indexes
        .find(left.as_ref())
        .and_then(|index_name| is_stateless(right.as_ref()).as_some(index_name))
    {
        Searched::Found {
            index_name,
            index_op,
            index_value_expr: *right,
            selection: None,
        }
    } else if let Some(index_name) = indexes
        .find(right.as_ref())
        .and_then(|index_name| is_stateless(left.as_ref()).as_some(index_name))
    {
        Searched::Found {
            index_name,
            index_op: index_op.reverse(),
            index_value_expr: *left,
            selection: None,
        }
    } else {
        Searched::NotFound(Expr::BinaryOp {
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
        _ => false,
    }
}
