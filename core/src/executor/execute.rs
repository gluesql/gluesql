use {
    super::{
        alter::{
            CreateTableOptions, alter_table, create_table, delete_function, drop_table,
            insert_function,
        },
        context::StatementContext,
        delete::delete,
        fetch::fetch,
        insert::insert,
        query, select,
        update::Update,
        validate::{ColumnValidation, validate_unique},
    },
    crate::{
        ast::{BinaryOperator, DataType, Dictionary, Literal, Variable},
        data::{Key, Row, SCHEMALESS_DOC_COLUMN, Value},
        plan::{
            DictionarySourcePlan, ExprPlan, FilterInputPlan, FilterPlan, ProjectInputPlan,
            ProjectPlan, ProjectionPlan, QueryPlan, SchemaMapPlan, SchemaPlan, SelectItemPlan,
            SourcePlan, StatementPlan, StatementPlanKind, TableAliasPlan,
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
    #[error("unplanned schema map")]
    UnplannedSchemaMap,

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

/// Executes a prepared statement without planning or automatic transaction handling.
///
/// Use [`Glue::execute_prepared`](crate::prelude::Glue::execute_prepared) to manage the
/// transaction automatically. SQL transaction statements still perform their
/// explicit BEGIN, COMMIT, or ROLLBACK operation.
pub fn execute<T: GStore + GStoreMut>(
    storage: &mut T,
    statement: &StatementPlan,
) -> Result<Payload> {
    let SchemaMapPlan::Planned(schema_map) = &statement.schema_map else {
        return Err(ExecuteError::UnplannedSchemaMap.into());
    };

    let stmt_context = StatementContext {
        functions: &statement.functions,
        schema_map,
    };

    match &statement.kind {
        //- Modification
        //-- Tables
        StatementPlanKind::CreateTable {
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
                columns,
                if_not_exists: *if_not_exists,
                source,
                engine,
                foreign_keys,
                comment,
            };

            create_table(storage, stmt_context, options).map(|()| Payload::Create)
        }
        StatementPlanKind::DropTable {
            names,
            if_exists,
            cascade,
            ..
        } => drop_table(storage, stmt_context, names, *if_exists, *cascade).map(Payload::DropTable),
        StatementPlanKind::AlterTable { name, operation } => {
            alter_table(storage, stmt_context, name, operation).map(|()| Payload::AlterTable)
        }
        StatementPlanKind::CreateIndex {
            name,
            table_name,
            column,
        } => storage
            .create_index(table_name, name, column)
            .map(|()| Payload::CreateIndex),
        StatementPlanKind::DropIndex { name, table_name } => storage
            .drop_index(table_name, name)
            .map(|()| Payload::DropIndex),
        //- Transaction
        StatementPlanKind::StartTransaction => {
            storage.begin(false).map(|_| Payload::StartTransaction)
        }
        StatementPlanKind::Commit => storage.commit().map(|()| Payload::Commit),
        StatementPlanKind::Rollback => storage.rollback().map(|()| Payload::Rollback),
        //-- Rows
        StatementPlanKind::Insert {
            table_name,
            columns,
            source,
        } => insert(storage, stmt_context, table_name, columns, source).map(Payload::Insert),
        StatementPlanKind::Update {
            table_name,
            selection,
            assignments,
        } => {
            let SchemaPlan {
                column_defs,
                foreign_keys,
                ..
            } = stmt_context
                .schema_map
                .get(table_name)
                .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

            let all_columns = column_defs.as_deref().map_or_else(
                || Rc::from(vec![SCHEMALESS_DOC_COLUMN.to_owned()]),
                |columns| columns.iter().map(|col_def| col_def.name.clone()).collect(),
            );
            let columns_to_update: Vec<String> = assignments
                .iter()
                .map(|assignment| assignment.id.clone())
                .collect();

            let update = Update::new(
                storage,
                stmt_context,
                table_name,
                assignments,
                column_defs.as_deref(),
            )?;

            let foreign_keys = Rc::new(foreign_keys.clone());

            let rows = fetch(
                storage,
                stmt_context,
                table_name,
                all_columns,
                selection.as_ref(),
            )?
            .map(|item| {
                let (key, row) = item?;
                let row = update.apply(row, foreign_keys.as_ref())?;

                Ok((key, row))
            })
            .collect::<Result<Vec<(Key, Row)>>>()?;

            if let Some(column_defs) = column_defs {
                let column_validation =
                    ColumnValidation::SpecifiedColumns(column_defs, columns_to_update);
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
        StatementPlanKind::Delete {
            table_name,
            selection,
        } => delete(storage, stmt_context, table_name, selection.as_ref()),

        //- Selection
        StatementPlanKind::Query(query) => select::execute(storage, stmt_context, query),
        StatementPlanKind::ShowColumns { table_name } => {
            let SchemaPlan { column_defs, .. } = stmt_context
                .schema_map
                .get(table_name)
                .ok_or_else(|| ExecuteError::TableNotFound(table_name.to_owned()))?;

            let output: Vec<(String, DataType)> = column_defs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|key| (key.name.clone(), key.data_type.clone()))
                .collect();

            Ok(Payload::ShowColumns(output))
        }
        StatementPlanKind::ShowIndexes(table_name) => {
            let query = QueryPlan::Project(ProjectPlan {
                projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Wildcard]),
                input: ProjectInputPlan::Filter(FilterPlan {
                    input: FilterInputPlan::Source(SourcePlan::Dictionary(DictionarySourcePlan {
                        dictionary: Dictionary::GlueIndexes,
                        alias: TableAliasPlan {
                            name: "GLUE_INDEXES".to_owned(),
                            columns: Vec::new(),
                        },
                    })),
                    expr: ExprPlan::BinaryOp {
                        left: Box::new(ExprPlan::Identifier("TABLE_NAME".to_owned())),
                        op: BinaryOperator::Eq,
                        right: Box::new(ExprPlan::Literal(Literal::QuotedString(
                            table_name.to_owned(),
                        ))),
                    },
                }),
            });

            let (labels, rows) = query::execute_with_labels(storage, stmt_context, None, &query)?;
            let rows = rows
                .map(|row| Ok::<_, Error>(row?.into_values()))
                .collect::<Result<Vec<_>>>()?;

            if rows.is_empty() {
                return Err(ExecuteError::TableNotFound(table_name.to_owned()).into());
            }

            Ok(Payload::Select { labels, rows })
        }
        StatementPlanKind::ShowVariable(variable) => match variable {
            Variable::Tables => {
                let query = QueryPlan::Project(ProjectPlan {
                    projection: ProjectionPlan::SelectItems(vec![SelectItemPlan::Expr {
                        expr: ExprPlan::Identifier("TABLE_NAME".to_owned()),
                        label: "TABLE_NAME".to_owned(),
                    }]),
                    input: ProjectInputPlan::Source(SourcePlan::Dictionary(DictionarySourcePlan {
                        dictionary: Dictionary::GlueTables,
                        alias: TableAliasPlan {
                            name: "GLUE_TABLES".to_owned(),
                            columns: Vec::new(),
                        },
                    })),
                });

                let table_names = query::execute(storage, stmt_context, None, &query)?
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
        StatementPlanKind::CreateFunction {
            or_replace,
            name,
            args,
            return_,
        } => insert_function(storage, name, args, *or_replace, return_).map(|()| Payload::Create),
        StatementPlanKind::DropFunction { if_exists, names } => {
            delete_function(storage, names, *if_exists).map(|()| Payload::DropFunction)
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::ExecuteError,
        crate::{
            mock::MockStorage,
            query_builder::{Build, table},
            result::Error,
        },
    };

    #[test]
    fn execute() {
        let mut storage = MockStorage::default();
        let statement = table("Item").select().build().unwrap();
        let actual = super::execute(&mut storage, &statement);
        let expected = Err(Error::Execute(ExecuteError::UnplannedSchemaMap));
        assert_eq!(actual, expected);
    }
}
