use {
    crate::{
        ast::{
            BinaryOperator, Expr, Query, Select, SetExpr, Statement, TableFactor, TableWithJoins,
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
            index_value_expr,
            selection,
        } => {
            let TableWithJoins { relation, joins } = from;
            let (name, alias) = match relation {
                TableFactor::Table { name, alias, .. } => (name, alias),
            };

            let index = Some((index_name, index_value_expr));
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
        index_value_expr: Box<Expr>,
        selection: Option<Expr>,
    },
    NotFound(Option<Expr>),
}

fn plan_selection(indexes: &[(String, Expr)], selection: Expr) -> Planned {
    for (index_name, index_expr) in indexes.iter() {
        if let Searched::Found {
            index_value_expr,
            selection,
        } = search_index(index_expr, &selection)
        {
            return Planned::Found {
                index_name: index_name.to_owned(),
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
        selection: Option<Expr>,
    },
    NotFound,
}

fn search_index(index_expr: &Expr, selection: &Expr) -> Searched {
    match selection {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if index_expr == left.as_ref() && is_stateless(right.as_ref()) {
                Searched::Found {
                    index_value_expr: right.clone(),
                    selection: None,
                }
            } else if index_expr == right.as_ref() && is_stateless(left.as_ref()) {
                Searched::Found {
                    index_value_expr: left.clone(),
                    selection: None,
                }
            } else {
                Searched::NotFound
            }
        }
        _ => Searched::NotFound,
    }
}

fn is_stateless(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}
