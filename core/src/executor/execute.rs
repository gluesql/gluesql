use {
    super::{
        alter::{create_table, drop_table},
        fetch::{fetch, fetch_columns},
        insert::insert,
        select::{select, select_with_labels},
        update::Update,
        validate::{validate_unique, ColumnValidation},
    },
    crate::{
        ast::{
            DataType, Dictionary, Expr, Query, SelectItem, SetExpr, Statement, TableAlias,
            TableFactor, TableWithJoins, Variable,
        },
        data::{Key, Row, Schema, Value},
        result::{MutResult, TrySelf},
        store::{GStore, GStoreMut},
    },
    futures::stream::{StreamExt, TryStreamExt},
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, env::var, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[cfg(feature = "alter-table")]
use super::alter::alter_table;

#[cfg(feature = "index")]
use {
    super::alter::create_index,
    crate::ast::{AstLiteral, BinaryOperator},
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum ExecuteError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Payload {
    ShowColumns(Vec<(String, DataType)>),
    Create,
    Insert(usize),
    Select {
        labels: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    SelectMap(Vec<HashMap<String, Value>>),
    Delete(usize),
    Update(usize),
    DropTable,

    #[cfg(feature = "alter-table")]
    AlterTable,

    #[cfg(feature = "index")]
    CreateIndex,
    #[cfg(feature = "index")]
    DropIndex,
    #[cfg(feature = "transaction")]
    StartTransaction,
    #[cfg(feature = "transaction")]
    Commit,
    #[cfg(feature = "transaction")]
    Rollback,

    ShowVariable(PayloadVariable),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PayloadVariable {
    Tables(Vec<String>),
    Version(String),
}

#[cfg(feature = "transaction")]
pub async fn execute_atomic<T: GStore + GStoreMut>(
    storage: T,
    statement: &Statement,
) -> MutResult<T, Payload> {
    if matches!(
        statement,
        Statement::StartTransaction | Statement::Rollback | Statement::Commit
    ) {
        return execute(storage, statement).await;
    }

    let (storage, autocommit) = storage.begin(true).await?;
    let result = execute(storage, statement).await;

    match (result, autocommit) {
        (Ok((storage, payload)), true) => {
            let (storage, ()) = storage.commit().await?;

            Ok((storage, payload))
        }
        (Err((storage, error)), true) => {
            let (storage, ()) = storage.rollback().await?;

            Err((storage, error))
        }
        (result, _) => result,
    }
}

pub async fn execute<T: GStore + GStoreMut>(
    mut storage: T,
    statement: &Statement,
) -> MutResult<T, Payload> {
    macro_rules! try_block {
        ($storage: expr, $block: block) => {{
            match (|| async { $block })().await {
                Err(e) => {
                    return Err(($storage, e));
                }
                Ok(v) => v,
            }
        }};
    }

    match statement {
        //- Modification
        //-- Tables
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            source,
            ..
        } => create_table(&mut storage, name, columns, *if_not_exists, source)
            .await
            .map(|_| Payload::Create)
            .try_self(storage),
        Statement::DropTable {
            names, if_exists, ..
        } => drop_table(&mut storage, names, *if_exists)
            .await
            .map(|_| Payload::DropTable)
            .try_self(storage),
        #[cfg(feature = "alter-table")]
        Statement::AlterTable { name, operation } => alter_table(storage, name, operation)
            .await
            .map(|(storage, _)| (storage, Payload::AlterTable)),
        #[cfg(feature = "index")]
        Statement::CreateIndex {
            name,
            table_name,
            column,
        } => create_index(storage, table_name, name, column)
            .await
            .map(|(storage, _)| (storage, Payload::CreateIndex)),
        #[cfg(feature = "index")]
        Statement::DropIndex { name, table_name } => storage
            .drop_index(table_name, name)
            .await
            .map(|(storage, _)| (storage, Payload::DropIndex)),
        //- Transaction
        #[cfg(feature = "transaction")]
        Statement::StartTransaction => storage
            .begin(false)
            .await
            .map(|(storage, _)| (storage, Payload::StartTransaction)),
        #[cfg(feature = "transaction")]
        Statement::Commit => storage
            .commit()
            .await
            .map(|(storage, _)| (storage, Payload::Commit)),
        #[cfg(feature = "transaction")]
        Statement::Rollback => storage
            .rollback()
            .await
            .map(|(storage, _)| (storage, Payload::Rollback)),
        //-- Rows
        Statement::Insert {
            table_name,
            columns,
            source,
        } => insert(&mut storage, table_name, columns, source)
            .await
            .map(Payload::Insert)
            .try_self(storage),
        Statement::Update {
            table_name,
            selection,
            assignments,
        } => {
            let (table_name, rows) = try_block!(storage, {
                let Schema { column_defs, .. } = storage
                    .fetch_schema(table_name)
                    .await?
                    .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

                let all_columns = column_defs.as_deref().map(|columns| {
                    columns
                        .iter()
                        .map(|col_def| col_def.name.to_owned())
                        .collect()
                });
                let columns_to_update = assignments
                    .iter()
                    .map(|assignment| assignment.id.to_owned())
                    .collect();

                let update =
                    Update::new(&storage, table_name, assignments, column_defs.as_deref())?;

                let rows = fetch(&storage, table_name, all_columns, selection.as_ref())
                    .await?
                    .and_then(|item| {
                        let update = &update;
                        let (key, row) = item;

                        async move {
                            let row = update.apply(row).await?;

                            Ok((key, row))
                        }
                    })
                    .try_collect::<Vec<(Key, Row)>>()
                    .await?;

                if let Some(column_defs) = column_defs {
                    let column_validation = ColumnValidation::SpecifiedColumns(
                        Rc::from(column_defs),
                        columns_to_update,
                    );
                    let rows = rows.iter().filter_map(|(_, row)| match row {
                        Row::Vec { values, .. } => Some(values.as_slice()),
                        Row::Map(_) => None,
                    });

                    validate_unique(&storage, table_name, column_validation, rows).await?;
                }

                Ok((table_name, rows))
            });

            let num_rows = rows.len();
            let rows = rows
                .into_iter()
                .map(|(key, row)| (key, row.into()))
                .collect();

            storage
                .insert_data(table_name, rows)
                .await
                .map(|_| Payload::Update(num_rows))
                .try_self(storage)
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let (table_name, keys) = try_block!(storage, {
                let columns = fetch_columns(&storage, table_name).await?.map(Rc::from);
                let keys = fetch(&storage, table_name, columns, selection.as_ref())
                    .await?
                    .map_ok(|(key, _)| key)
                    .try_collect::<Vec<_>>()
                    .await?;

                Ok((table_name, keys))
            });

            let num_keys = keys.len();

            storage
                .delete_data(table_name, keys)
                .await
                .map(|_| Payload::Delete(num_keys))
                .try_self(storage)
        }

        //- Selection
        Statement::Query(query) => {
            let payload = try_block!(storage, {
                let (labels, rows) = select_with_labels(&storage, query, None).await?;

                match labels {
                    Some(labels) => rows
                        .map(|row| row?.try_into_vec())
                        .try_collect::<Vec<_>>()
                        .await
                        .map(|rows| Payload::Select { labels, rows }),
                    None => rows
                        .map(|row| row?.try_into_map())
                        .try_collect::<Vec<_>>()
                        .await
                        .map(Payload::SelectMap),
                }
            });

            Ok((storage, payload))
        }
        Statement::ShowColumns { table_name } => {
            let keys = try_block!(storage, {
                let Schema { column_defs, .. } = storage
                    .fetch_schema(table_name)
                    .await?
                    .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

                Ok(column_defs)
            });

            let output: Vec<(String, DataType)> = keys
                .unwrap_or_default()
                .into_iter()
                .map(|key| (key.name, key.data_type))
                .collect();

            Ok((storage, Payload::ShowColumns(output)))
        }
        #[cfg(feature = "index")]
        Statement::ShowIndexes(table_name) => {
            let query = Query {
                body: SetExpr::Select(Box::new(crate::ast::Select {
                    projection: vec![SelectItem::Wildcard],
                    from: TableWithJoins {
                        relation: TableFactor::Dictionary {
                            dict: Dictionary::GlueIndexes,
                            alias: TableAlias {
                                name: "GLUE_INDEXES".to_owned(),
                                columns: Vec::new(),
                            },
                        },
                        joins: Vec::new(),
                    },
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier("TABLE_NAME".to_owned())),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Literal(AstLiteral::QuotedString(
                            table_name.to_owned(),
                        ))),
                    }),
                    group_by: Vec::new(),
                    having: None,
                })),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };

            let payload = try_block!(storage, {
                let (labels, rows) = select_with_labels(&storage, &query, None).await?;
                let labels = labels.unwrap_or_default();
                let rows = rows
                    .map(|row| row?.try_into_vec())
                    .try_collect::<Vec<_>>()
                    .await?;

                if rows.is_empty() {
                    return Err(ExecuteError::TableNotFound(table_name.to_owned()).into());
                }

                Ok(Payload::Select { labels, rows })
            });

            Ok((storage, payload))
        }
        Statement::ShowVariable(variable) => match variable {
            Variable::Tables => {
                let query = Query {
                    body: SetExpr::Select(Box::new(crate::ast::Select {
                        projection: vec![SelectItem::Expr {
                            expr: Expr::Identifier("TABLE_NAME".to_owned()),
                            label: "TABLE_NAME".to_owned(),
                        }],
                        from: TableWithJoins {
                            relation: TableFactor::Dictionary {
                                dict: Dictionary::GlueTables,
                                alias: TableAlias {
                                    name: "GLUE_TABLES".to_owned(),
                                    columns: Vec::new(),
                                },
                            },
                            joins: Vec::new(),
                        },
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                    })),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                };

                let rows = try_block!(storage, {
                    select(&storage, &query, None)
                        .await?
                        .map(|row| row?.try_into_vec())
                        .try_collect::<Vec<_>>()
                        .await
                });

                let table_names = rows
                    .iter()
                    .flat_map(|values| values.iter().map(|value| value.into()))
                    .collect::<Vec<_>>();

                Ok((
                    storage,
                    Payload::ShowVariable(PayloadVariable::Tables(table_names)),
                ))
            }
            Variable::Version => {
                let version = var("CARGO_PKG_VERSION")
                    .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned());
                let payload = Payload::ShowVariable(PayloadVariable::Version(version));

                Ok((storage, payload))
            }
        },
    }
}
