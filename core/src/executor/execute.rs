use {
    super::{
        alter::{
            alter_table, create_index, create_table, delete_function, drop_table, insert_function,
            CreateTableOptions,
        },
        delete::delete,
        insert::insert,
        select::{select, select_with_labels},
        update::{self},
    },
    crate::{
        ast::{
            AstLiteral, BinaryOperator, DataType, Dictionary, Expr, Query, SelectItem, SetExpr,
            Statement, TableAlias, TableFactor, TableWithJoins, Variable,
        },
        data::{Schema, Value},
        result::Result,
        store::{GStore, GStoreMut},
    },
    futures::stream::{StreamExt, TryStreamExt},
    serde::{Deserialize, Serialize},
    std::{collections::HashMap, env::var, fmt::Debug},
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
    DropTable,
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

    /// Comulates the number of affected rows in the payload, when the provided payload is of the same type.
    ///
    /// # Arguments
    /// * `other` - The other payload to be added.
    ///
    /// # Example
    /// ```
    /// use gluesql_core::executor::Payload;
    ///
    /// let mut payload = Payload::Insert(1);
    /// payload.accumulate(&Payload::Insert(2));
    ///
    /// assert_eq!(payload, Payload::Insert(3));
    /// ```
    ///
    /// # Panics
    /// Panics if the payloads are not of the same type.
    ///
    /// ```should_panic
    /// use gluesql_core::executor::Payload;
    ///
    /// let mut payload = Payload::Insert(1);
    ///
    /// payload.accumulate(&Payload::Delete(2));
    /// ```
    ///
    pub fn accumulate(&mut self, other: &Self) {
        match (self, other) {
            (Payload::Insert(a), Payload::Insert(b)) => *a += b,
            (Payload::Delete(a), Payload::Delete(b)) => *a += b,
            (Payload::Update(a), Payload::Update(b)) => *a += b,
            _ => {
                unreachable!("accumulate is only for Insert, Delete, and Update")
            }
        }
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
            primary_key,
            comment,
        } => {
            let options = CreateTableOptions {
                target_table_name: name,
                column_defs: columns.as_ref().map(Vec::as_slice),
                if_not_exists: *if_not_exists,
                source,
                engine,
                foreign_keys,
                primary_key,
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
            .map(|_| Payload::DropTable),
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
        } => update::update(storage, table_name, selection, assignments).await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Value;

    #[test]
    fn test_payload_select() {
        let payload = Payload::Select {
            labels: vec!["a".to_owned(), "b".to_owned()],
            rows: vec![vec![Value::U8(1), Value::U8(2)]],
        };

        let mut iter = payload.select().unwrap();
        let row = iter.next().unwrap();

        assert_eq!(row.get("a"), Some(&&Value::U8(1)));
        assert_eq!(row.get("b"), Some(&&Value::U8(2)));
    }

    #[test]
    fn test_payload_select_map() {
        let payload = Payload::SelectMap(vec![{
            let mut map = HashMap::new();
            map.insert("a".to_owned(), Value::U8(1));
            map.insert("b".to_owned(), Value::U8(2));
            map
        }]);

        let mut iter = payload.select().unwrap();
        let row = iter.next().unwrap();

        assert_eq!(row.get("a"), Some(&&Value::U8(1)));
        assert_eq!(row.get("b"), Some(&&Value::U8(2)));
    }

    #[test]
    fn test_payload_accumulate() {
        let mut payload = Payload::Insert(1);
        payload.accumulate(&Payload::Insert(2));

        assert_eq!(payload, Payload::Insert(3));
    }

    #[test]
    #[should_panic]
    fn test_payload_accumulate_panic() {
        let mut payload = Payload::Insert(1);
        payload.accumulate(&Payload::Delete(2));
    }
}
