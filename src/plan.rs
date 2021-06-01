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
        index_value_expr: Box<Expr>,
        selection: Option<Expr>,
    },
    NotFound(Option<Expr>),
}

fn plan_selection(indexes: &[(String, Expr)], selection: Expr) -> Planned {
    for (index_name, index_expr) in indexes.iter() {
        if let Searched::Found {
            index_value_expr,
            index_op,
            selection,
        } = search_index(index_expr, &selection)
        {
            return Planned::Found {
                index_name: index_name.to_owned(),
                index_op,
                index_value_expr,
                selection,
            };
        }
    }

    Planned::NotFound(Some(selection))
}

enum Searched {
    Found {
        index_value_expr: Box<Expr>,
        index_op: IndexOperator,
        selection: Option<Expr>,
    },
    NotFound,
}

fn search_index(index_expr: &Expr, selection: &Expr) -> Searched {
    match selection {
        Expr::BinaryOp {
            op: BinaryOperator::Gt,
            left,
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
        _ => Searched::NotFound,
    }
}

fn search_binary_op(
    index_expr: &Expr,
    index_op: IndexOperator,
    left: &Expr,
    right: &Expr,
) -> Searched {
    if index_expr == left && is_stateless(right) {
        Searched::Found {
            index_value_expr: Box::new(right.clone()),
            index_op,
            selection: None,
        }
    } else if index_expr == right && is_stateless(left) {
        Searched::Found {
            index_value_expr: Box::new(left.clone()),
            index_op: index_op.reverse(),
            selection: None,
        }
    } else {
        Searched::NotFound
    }
}

fn is_stateless(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}
