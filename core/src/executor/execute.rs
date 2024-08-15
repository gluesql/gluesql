use {
    super::{
        alter::{
            alter_table, create_index, create_table, delete_function, drop_table, insert_function,
            CreateTableOptions,
        },
        delete::delete,
        fetch::fetch,
        insert::insert,
        select::{select, select_with_labels},
        update::Update,
        validate::{validate_unique, ColumnValidation},
    },
    crate::{
        ast::{
            AstLiteral, BinaryOperator, DataType, Dictionary, Expr, Query, SelectItem, SetExpr,
            Statement, TableAlias, TableFactor, TableWithJoins, Variable,
        },
        data::{Key, Row, Schema, Value},
        result::Result,
        store::{GStore, GStoreMut},
    },
    futures::stream::{StreamExt, TryStreamExt},
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, env::var, fmt::Debug, rc::Rc},
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum ExecuteError {
    #[error("table not found: {0}")]
    TableNotFound(String),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    DropTable(usize),
    DropFunction,
    AlterTable,
    CreateIndex,
    DropIndex,
    StartTransaction,
    Commit,
    Rollback,
    ShowVariable(PayloadVariable),
}

impl Payload {
    /// Exports `select` payloads as an [`std::iter::Iterator`].
    ///
    /// The items of the Iterator are `HashMap<Column, Value>`, and they are borrowed by default.
    /// If ownership is required, you need to acquire them directly.
    ///
    /// - Some: [`Payload::Select`], [`Payload::SelectMap`]
    /// - None: otherwise
    pub fn select(&self) -> Option<impl Iterator<Item = HashMap<&str, &Value>>> {
        #[derive(iter_enum::Iterator)]
        enum Iter<I1, I2> {
            Schema(I1),
            Schemaless(I2),
        }

        Some(match self {
            Payload::Select { labels, rows } => Iter::Schema(rows.iter().map(move |row| {
                labels
                    .iter()
                    .zip(row.iter())
                    .map(|(label, value)| (label.as_str(), value))
                    .collect::<HashMap<_, _>>()
            })),
            Payload::SelectMap(rows) => Iter::Schemaless(rows.iter().map(|row| {
                row.iter()
                    .map(|(k, v)| (k.as_str(), v))
                    .collect::<HashMap<_, _>>()
            })),
            _ => return None,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum PayloadVariable {
    Tables(Vec<String>),
    Functions(Vec<String>),
    Version(String),
}

pub async fn execute<T: GStore + GStoreMut>(
    storage: &mut T,
    statement: &Statement,
) -> Result<Payload> {
    if matches!(
        statement,
        Statement::StartTransaction | Statement::Rollback | Statement::Commit
    ) {
        return execute_inner(storage, statement).await;
    }

    let autocommit = storage.begin(true).await?;
    let result = execute_inner(storage, statement).await;

    if !autocommit {
        return result;
    }

    match result {
        Ok(payload) => storage.commit().await.map(|_| payload),
        Err(error) => {
            storage.rollback().await?;

            Err(error)
        }
    }
}

async fn execute_inner<T: GStore + GStoreMut>(
    storage: &mut T,
    statement: &Statement,
) -> Result<Payload> {
    match statement {
        //- Modification
        //-- Tables
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            source,
            engine,
            foreign_keys,
            comment,
        } => {
            let options = CreateTableOptions {
                target_table_name: name,
                column_defs: columns.as_ref().map(Vec::as_slice),
                if_not_exists: *if_not_exists,
                source,
                engine,
                foreign_keys,
                comment,
            };

            create_table(storage, options)
                .await
                .map(|_| Payload::Create)
        }
        Statement::DropTable {
            names,
            if_exists,
            cascade,
            ..
        } => drop_table(storage, names, *if_exists, *cascade)
            .await
            .map(Payload::DropTable),
        Statement::AlterTable { name, operation } => alter_table(storage, name, operation)
            .await
            .map(|_| Payload::AlterTable),
        Statement::CreateIndex {
            name,
            table_name,
            column,
        } => create_index(storage, table_name, name, column)
            .await
            .map(|_| Payload::CreateIndex),
        Statement::DropIndex { name, table_name } => storage
            .drop_index(table_name, name)
            .await
            .map(|_| Payload::DropIndex),
        //- Transaction
        Statement::StartTransaction => storage
            .begin(false)
            .await
            .map(|_| Payload::StartTransaction),
        Statement::Commit => storage.commit().await.map(|_| Payload::Commit),
        Statement::Rollback => storage.rollback().await.map(|_| Payload::Rollback),
        //-- Rows
        Statement::Insert {
            table_name,
            columns,
            source,
        } => insert(storage, table_name, columns, source)
            .await
            .map(Payload::Insert),
        Statement::Update {
            table_name,
            selection,
            assignments,
        } => {
            let Schema {
                column_defs,
                foreign_keys,
                ..
            } = storage
                .fetch_schema(table_name)
                .await?
                .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

            let all_columns = column_defs.as_deref().map(|columns| {
                columns
                    .iter()
                    .map(|col_def| col_def.name.to_owned())
                    .collect()
            });
            let columns_to_update: Vec<String> = assignments
                .iter()
                .map(|assignment| assignment.id.to_owned())
                .collect();

            let update = Update::new(storage, table_name, assignments, column_defs.as_deref())?;

            let foreign_keys = Rc::new(foreign_keys);

            let rows = fetch(storage, table_name, all_columns, selection.as_ref())
                .await?
                .and_then(|item| {
                    let update = &update;
                    let (key, row) = item;

                    let foreign_keys = Rc::clone(&foreign_keys);
                    async move {
                        let row = update.apply(row, foreign_keys.as_ref()).await?;

                        Ok((key, row))
                    }
                })
                .try_collect::<Vec<(Key, Row)>>()
                .await?;

            if let Some(column_defs) = column_defs {
                let column_validation =
                    ColumnValidation::SpecifiedColumns(&column_defs, columns_to_update);
                let rows = rows.iter().filter_map(|(_, row)| match row {
                    Row::Vec { values, .. } => Some(values.as_slice()),
                    Row::Map(_) => None,
                });

                validate_unique(storage, table_name, column_validation, rows).await?;
            }

            let num_rows = rows.len();
            let rows = rows
                .into_iter()
                .map(|(key, row)| (key, row.into()))
                .collect();

            storage
                .insert_data(table_name, rows)
                .await
                .map(|_| Payload::Update(num_rows))
        }
        Statement::Delete {
            table_name,
            selection,
        } => delete(storage, table_name, selection).await,

        //- Selection
        Statement::Query(query) => {
            let (labels, rows) = select_with_labels(storage, query, None).await?;

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
        }
        Statement::ShowColumns { table_name } => {
            let Schema { column_defs, .. } = storage
                .fetch_schema(table_name)
                .await?
                .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

            let output: Vec<(String, DataType)> = column_defs
                .unwrap_or_default()
                .into_iter()
                .map(|key| (key.name, key.data_type))
                .collect();

            Ok(Payload::ShowColumns(output))
        }
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

            let (labels, rows) = select_with_labels(storage, &query, None).await?;
            let labels = labels.unwrap_or_default();
            let rows = rows
                .map(|row| row?.try_into_vec())
                .try_collect::<Vec<_>>()
                .await?;

            if rows.is_empty() {
                return Err(ExecuteError::TableNotFound(table_name.to_owned()).into());
            }

            Ok(Payload::Select { labels, rows })
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

                let table_names = select(storage, &query, None)
                    .await?
                    .map(|row| row?.try_into_vec())
                    .try_collect::<Vec<Vec<Value>>>()
                    .await?
                    .iter()
                    .flat_map(|values| values.iter().map(|value| value.into()))
                    .collect::<Vec<_>>();

                Ok(Payload::ShowVariable(PayloadVariable::Tables(table_names)))
            }
            Variable::Functions => {
                let mut function_desc: Vec<_> = storage
                    .fetch_all_functions()
                    .await?
                    .iter()
                    .map(|f| f.to_str())
                    .collect();
                function_desc.sort();
                Ok(Payload::ShowVariable(PayloadVariable::Functions(
                    function_desc,
                )))
            }
            Variable::Version => {
                let version = var("CARGO_PKG_VERSION")
                    .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned());
                let payload = Payload::ShowVariable(PayloadVariable::Version(version));

                Ok(payload)
            }
        },
        Statement::CreateFunction {
            or_replace,
            name,
            args,
            return_,
        } => insert_function(storage, name, args, *or_replace, return_)
            .await
            .map(|_| Payload::Create),
        Statement::DropFunction { if_exists, names } => delete_function(storage, names, *if_exists)
            .await
            .map(|_| Payload::DropFunction),
    }
}
