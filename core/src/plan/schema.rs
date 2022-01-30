use {
    crate::{
        ast::{Expr, Query, Select, SetExpr, Statement, TableFactor, TableWithJoins},
        data::{get_name, Schema},
        result::Result,
        store::Store,
    },
    async_recursion::async_recursion,
    std::{collections::HashMap, fmt::Debug},
};

pub async fn fetch_schema_map<T: Debug>(
    storage: &dyn Store<T>,
    statement: &Statement,
) -> Result<HashMap<String, Schema>> {
    match statement {
        Statement::Query(query) => scan_query(storage, query).await.map(|schema_list| {
            schema_list
                .into_iter()
                .map(|schema| (schema.table_name.clone(), schema))
                .collect::<HashMap<_, _>>()
        }),
        _ => Ok(HashMap::new()),
    }
}

async fn scan_query<T: Debug>(storage: &dyn Store<T>, query: &Query) -> Result<Vec<Schema>> {
    let Query { body, .. } = query;

    match body {
        SetExpr::Select(select) => scan_select(storage, select).await,
        SetExpr::Values(_) => Ok(Vec::new()),
    }
}

async fn scan_select<T: Debug>(storage: &dyn Store<T>, select: &Select) -> Result<Vec<Schema>> {
    let Select {
        from, selection, ..
    } = select;

    let schema_list = scan_table_with_joins(storage, from).await?;
    let schema_list = match selection {
        Some(expr) => scan_expr(storage, expr)
            .await?
            .into_iter()
            .chain(schema_list)
            .collect(),
        None => schema_list,
    };

    Ok(schema_list)
}

async fn scan_table_with_joins<T: Debug>(
    storage: &dyn Store<T>,
    table_with_joins: &TableWithJoins,
) -> Result<Vec<Schema>> {
    let TableWithJoins { relation, .. } = table_with_joins;
    let table_name = match relation {
        TableFactor::Table { name, .. } => name,
    };
    let table_name = get_name(table_name)?;
    let schema = storage.fetch_schema(table_name).await?;
    let schema_map = match schema {
        Some(schema) => vec![schema],
        None => vec![],
    };

    Ok(schema_map)
}

#[async_recursion(?Send)]
async fn scan_expr<T: Debug>(storage: &dyn Store<T>, expr: &Expr) -> Result<Vec<Schema>> {
    match expr {
        Expr::Nested(expr) | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            scan_expr(storage, expr).await
        }
        Expr::Subquery(query) | Expr::Exists(query) => scan_query(storage, query).await,
        Expr::InSubquery { expr, subquery, .. } => Ok(scan_expr(storage, expr)
            .await?
            .into_iter()
            .chain(scan_query(storage, subquery).await?)
            .collect()),
        Expr::BinaryOp { left, right, .. } => Ok(scan_expr(storage, left)
            .await?
            .into_iter()
            .chain(scan_expr(storage, right).await?)
            .collect()),
        _ => Ok(Vec::new()),
    }
}
