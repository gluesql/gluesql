use {
    super::{
        alter::{
            CreateTableOptions, alter_table, create_index, create_table, delete_function,
            drop_table, insert_function,
        },
        delete::delete,
        fetch::fetch,
        insert::insert,
        select::{select, select_with_labels},
        update::Update,
        validate::{ColumnValidation, validate_unique},
    },
    crate::{
        ast::{BinaryOperator, DataType, Dictionary, Literal, Variable},
        data::{Key, Row, SCHEMALESS_DOC_COLUMN, Schema, Value},
        plan::{
            ExprPlan, ProjectionPlan, QueryBodyPlan, QueryPlan, SelectItemPlan, SelectPlan,
            SetExprPlan, StatementPlan, TableAliasPlan, TableFactorPlan, TableWithJoinsPlan,
        },
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeMap, HashMap},
        env::var,
        fmt::Debug,
        rc::Rc,
    },
    thiserror::Error as ThisError,
};

#[derive(ThisError, Serialize, Debug, PartialEq, Eq)]
pub enum ExecuteError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("expected Map value in _doc column")]
    ExpectedMapValueInDocColumn,
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
    SelectMap(Vec<BTreeMap<String, Value>>),
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

pub fn execute<T: GStore + GStoreMut>(
    storage: &mut T,
    statement: &StatementPlan,
) -> Result<Payload> {
    if matches!(
        statement,
        StatementPlan::StartTransaction | StatementPlan::Rollback | StatementPlan::Commit
    ) {
        return execute_inner(storage, statement);
    }

    let autocommit = storage.begin(true)?;
    let result = execute_inner(storage, statement);

    if !autocommit {
        return result;
    }

    match result {
        Ok(payload) => storage.commit().map(|()| payload),
        Err(error) => {
            storage.rollback()?;

            Err(error)
        }
    }
}

fn execute_inner<T: GStore + GStoreMut>(
    storage: &mut T,
    statement: &StatementPlan,
) -> Result<Payload> {
    match statement {
        //- Modification
        //-- Tables
        StatementPlan::CreateTable {
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

            create_table(storage, options).map(|()| Payload::Create)
        }
        StatementPlan::DropTable {
            names,
            if_exists,
            cascade,
            ..
        } => drop_table(storage, names, *if_exists, *cascade).map(Payload::DropTable),
        StatementPlan::AlterTable { name, operation } => {
            alter_table(storage, name, operation).map(|()| Payload::AlterTable)
        }
        StatementPlan::CreateIndex {
            name,
            table_name,
            column,
        } => create_index(storage, table_name, name, column).map(|()| Payload::CreateIndex),
        StatementPlan::DropIndex { name, table_name } => storage
            .drop_index(table_name, name)
            .map(|()| Payload::DropIndex),
        //- Transaction
        StatementPlan::StartTransaction => storage.begin(false).map(|_| Payload::StartTransaction),
        StatementPlan::Commit => storage.commit().map(|()| Payload::Commit),
        StatementPlan::Rollback => storage.rollback().map(|()| Payload::Rollback),
        //-- Rows
        StatementPlan::Insert {
            table_name,
            columns,
            source,
        } => insert(storage, table_name, columns, source).map(Payload::Insert),
        StatementPlan::Update {
            table_name,
            selection,
            assignments,
        } => {
            let Schema {
                column_defs,
                foreign_keys,
                ..
            } = storage
                .fetch_schema(table_name)?
                .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

            let all_columns = column_defs.as_deref().map_or_else(
                || Rc::from(vec![SCHEMALESS_DOC_COLUMN.to_owned()]),
                |columns| columns.iter().map(|col_def| col_def.name.clone()).collect(),
            );
            let columns_to_update: Vec<String> = assignments
                .iter()
                .map(|assignment| assignment.id.clone())
                .collect();

            let update = Update::new(storage, table_name, assignments, column_defs.as_deref())?;

            let foreign_keys = Rc::new(foreign_keys);

            let rows = fetch(storage, table_name, all_columns, selection.as_ref())?
                .map(|item| {
                    let (key, row) = item?;
                    let row = update.apply(row, foreign_keys.as_ref())?;

                    Ok((key, row))
                })
                .collect::<Result<Vec<(Key, Row)>>>()?;

            if let Some(column_defs) = column_defs {
                let column_validation =
                    ColumnValidation::SpecifiedColumns(&column_defs, columns_to_update);
                let rows = rows.iter().map(|(_, row)| row.values.as_slice());

                validate_unique(storage, table_name, &column_validation, rows)?;
            }

            let num_rows = rows.len();
            let rows = rows
                .into_iter()
                .map(|(key, row)| (key, row.into_values()))
                .collect();

            storage
                .insert_data(table_name, rows)
                .map(|()| Payload::Update(num_rows))
        }
        StatementPlan::Delete {
            table_name,
            selection,
        } => delete(storage, table_name, selection.as_ref()),

        //- Selection
        StatementPlan::Query(query) => {
            let (labels, rows) = select_with_labels(storage, query, None)?;

            let is_schemaless_map = matches!(query.body(), SetExprPlan::Select(select)
                if matches!(select.projection, ProjectionPlan::SchemalessMap));

            if is_schemaless_map {
                rows.map(|row| {
                    let mut values = row?.into_values().into_iter();
                    match (values.next(), values.next()) {
                        (Some(Value::Map(map)), None) => Ok(map),
                        _ => Err(ExecuteError::ExpectedMapValueInDocColumn.into()),
                    }
                })
                .collect::<Result<Vec<_>>>()
                .map(Payload::SelectMap)
            } else {
                rows.map(|row| Ok(row?.into_values()))
                    .collect::<Result<Vec<_>>>()
                    .map(|rows| Payload::Select { labels, rows })
            }
        }
        StatementPlan::ShowColumns { table_name } => {
            let Schema { column_defs, .. } = storage
                .fetch_schema(table_name)?
                .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

            let output: Vec<(String, DataType)> = column_defs
                .unwrap_or_default()
                .into_iter()
                .map(|key| (key.name, key.data_type))
                .collect();

            Ok(Payload::ShowColumns(output))
        }
        StatementPlan::ShowIndexes(table_name) => {
            let query = QueryPlan::Body(QueryBodyPlan {
                body: SetExprPlan::Select(Box::new(SelectPlan {
                    distinct: false,
                    projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
                    from: TableWithJoinsPlan {
                        relation: TableFactorPlan::Dictionary {
                            dict: Dictionary::GlueIndexes,
                            alias: TableAliasPlan {
                                name: "GLUE_INDEXES".to_owned(),
                                columns: Vec::new(),
                            },
                        },
                        joins: Vec::new(),
                    },
                    selection: Some(ExprPlan::BinaryOp {
                        left: Box::new(ExprPlan::Identifier("TABLE_NAME".to_owned())),
                        op: BinaryOperator::Eq,
                        right: Box::new(ExprPlan::Literal(Literal::QuotedString(
                            table_name.to_owned(),
                        ))),
                    }),
                    group_by: Vec::new(),
                    having: None,
                    aggregate_slots: None,
                })),
                order_by: Vec::new(),
            });

            let (labels, rows) = select_with_labels(storage, &query, None)?;
            let rows = rows
                .map(|row| Ok::<_, Error>(row?.into_values()))
                .collect::<Result<Vec<_>>>()?;

            if rows.is_empty() {
                return Err(ExecuteError::TableNotFound(table_name.to_owned()).into());
            }

            Ok(Payload::Select { labels, rows })
        }
        StatementPlan::ShowVariable(variable) => match variable {
            Variable::Tables => {
                let query = QueryPlan::Body(QueryBodyPlan {
                    body: SetExprPlan::Select(Box::new(SelectPlan {
                        distinct: false,
                        projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                            expr: ExprPlan::Identifier("TABLE_NAME".to_owned()),
                            label: "TABLE_NAME".to_owned(),
                        }]),
                        from: TableWithJoinsPlan {
                            relation: TableFactorPlan::Dictionary {
                                dict: Dictionary::GlueTables,
                                alias: TableAliasPlan {
                                    name: "GLUE_TABLES".to_owned(),
                                    columns: Vec::new(),
                                },
                            },
                            joins: Vec::new(),
                        },
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                        aggregate_slots: None,
                    })),
                    order_by: Vec::new(),
                });

                let table_names = select(storage, &query, None)?
                    .map(|row| Ok::<_, Error>(row?.into_values()))
                    .collect::<Result<Vec<Vec<Value>>>>()?
                    .iter()
                    .flat_map(|values| values.iter().map(Into::into))
                    .collect::<Vec<_>>();

                Ok(Payload::ShowVariable(PayloadVariable::Tables(table_names)))
            }
            Variable::Functions => {
                let mut function_desc: Vec<_> = storage
                    .fetch_all_functions()?
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
        StatementPlan::CreateFunction {
            or_replace,
            name,
            args,
            return_,
        } => insert_function(storage, name, args, *or_replace, return_).map(|()| Payload::Create),
        StatementPlan::DropFunction { if_exists, names } => {
            delete_function(storage, names, *if_exists).map(|()| Payload::DropFunction)
        }
    }
}
