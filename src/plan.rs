use {
    crate::{
        ast::{
            BinaryOperator, Expr, IndexItem, IndexOperator, Query, Select, SetExpr, Statement,
            TableFactor, TableWithJoins,
        },
        data::{get_name, Schema},
        result::Result,
        store::Store,
    },
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
        Some(Schema { indexes, .. }) => indexes,
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

    let planned = match selection {
        Some(expr) => plan_selection(&indexes, expr),
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

    match planned {
        Planned::NotFound(selection) => Ok(Select {
            projection,
            from,
            selection,
            group_by,
            having,
        }),
        Planned::Found {
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

enum Planned {
    Found {
        index_name: String,
        index_op: IndexOperator,
        index_value_expr: Expr,
        selection: Option<Expr>,
    },
    NotFound(Option<Expr>),
}

impl From<(&str, Searched)> for Planned {
    fn from((index_name, searched): (&str, Searched)) -> Self {
        match searched {
            Searched::Found {
                index_op,
                index_value_expr,
                selection,
            } => Planned::Found {
                index_name: index_name.to_owned(),
                index_op,
                index_value_expr,
                selection,
            },
            Searched::NotFound(selection) => Planned::NotFound(selection),
        }
    }
}

fn plan_selection(indexes: &[(String, Expr)], selection: Expr) -> Planned {
    indexes.iter().fold(
        Planned::NotFound(Some(selection)),
        |planned, (index_name, index_expr)| {
            let selection = match planned {
                Planned::Found { .. } | Planned::NotFound(None) => {
                    return planned;
                }
                Planned::NotFound(Some(selection)) => selection,
            };

            let searched = search_index(index_expr, selection);

            Planned::from((index_name.as_str(), searched))
        },
    )
}

enum Searched {
    Found {
        index_value_expr: Expr,
        index_op: IndexOperator,
        selection: Option<Expr>,
    },
    NotFound(Option<Expr>),
}

fn search_index(index_expr: &Expr, selection: Expr) -> Searched {
    match selection {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Gt,
            right,
        } => search_binary_op(index_expr, IndexOperator::Gt, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Lt,
            right,
        } => search_binary_op(index_expr, IndexOperator::Lt, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::GtEq,
            right,
        } => search_binary_op(index_expr, IndexOperator::GtEq, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::LtEq,
            right,
        } => search_binary_op(index_expr, IndexOperator::LtEq, left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => search_binary_op(index_expr, IndexOperator::Eq, left, right),
        _ => Searched::NotFound(Some(selection)),
    }
}

fn search_binary_op(
    index_expr: &Expr,
    index_op: IndexOperator,
    left: Box<Expr>,
    right: Box<Expr>,
) -> Searched {
    if index_expr == left.as_ref() && is_stateless(right.as_ref()) {
        Searched::Found {
            index_value_expr: *right,
            index_op,
            selection: None,
        }
    } else if index_expr == right.as_ref() && is_stateless(left.as_ref()) {
        Searched::Found {
            index_value_expr: *left,
            index_op: index_op.reverse(),
            selection: None,
        }
    } else {
        Searched::NotFound(Some(Expr::BinaryOp {
            left,
            op: index_op.into(),
            right,
        }))
    }
}

fn is_stateless(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}
