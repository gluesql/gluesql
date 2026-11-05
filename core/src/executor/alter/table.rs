use {
    super::{AlterError, validate, validate_column_names},
    crate::{
        ast::{ColumnUniqueOption, ForeignKey, ToSql},
        data::{Row, Schema},
        executor::{
            evaluate::evaluate,
            query::{self, OutputBody},
        },
        plan::{ColumnDefPlan, CreateTableColumnsPlan, CustomFunctionPlan, QueryPlan, ValuesPlan},
        prelude::{DataType, Value},
        result::Result,
        store::{GStore, GStoreMut},
    },
    serde::Serialize,
    std::{collections::HashSet, fmt},
};

pub struct CreateTableOptions<'a> {
    pub target_table_name: &'a str,
    pub columns: &'a CreateTableColumnsPlan,
    pub if_not_exists: bool,
    pub source: &'a Option<Box<QueryPlan>>,
    pub engine: &'a Option<String>,
    pub foreign_keys: &'a Vec<ForeignKey>,
    pub comment: &'a Option<String>,
}

pub fn create_table<T: GStore + GStoreMut>(
    storage: &mut T,
    functions: &[CustomFunctionPlan],
    CreateTableOptions {
        target_table_name,
        columns,
        if_not_exists,
        source,
        engine,
        foreign_keys,
        comment,
    }: CreateTableOptions<'_>,
) -> Result<()> {
    let mut selected_source_rows = None;
    let target_columns_defs = match columns {
        CreateTableColumnsPlan::Unplanned { .. } => {
            return Err(
                AlterError::UnplannedCreateTableColumns(target_table_name.to_owned()).into(),
            );
        }
        _ if if_not_exists
            && source.is_some()
            && storage.fetch_schema(target_table_name)?.is_some() =>
        {
            return Ok(());
        }
        CreateTableColumnsPlan::Columns(columns) => Some(columns.clone()),
        CreateTableColumnsPlan::Schemaless => None,
        CreateTableColumnsPlan::FromQuery => {
            let source_query = source
                .as_deref()
                .ok_or(AlterError::MalformedPlanCreateTableSource)?;

            match query::output_body(source_query) {
                OutputBody::Project(_) => {
                    let (labels, rows) =
                        query::execute_with_labels(storage, functions, source_query, None)?;
                    let rows = rows
                        .map(|row| row.map(Row::into_values))
                        .collect::<Result<Vec<_>>>()?;
                    let column_defs = column_defs_from_rows(labels, &rows);
                    selected_source_rows = Some(rows);

                    Some(column_defs)
                }
                OutputBody::Values(ValuesPlan(values_list)) => {
                    let first_len = values_list[0].len();
                    let mut column_types = vec![None; first_len];

                    for exprs in values_list {
                        for (i, expr) in exprs.iter().enumerate() {
                            if column_types[i].is_some() {
                                continue;
                            }

                            column_types[i] = evaluate(storage, functions, None, None, expr)
                                .and_then(Value::try_from)
                                .map(|value| value.get_type())?;
                        }

                        if column_types.iter().all(Option::is_some) {
                            break;
                        }
                    }

                    let column_defs = column_types
                        .iter()
                        .map(|column_type| match column_type {
                            Some(column_type) => column_type.to_owned(),
                            None => DataType::Text,
                        })
                        .enumerate()
                        .map(|(i, data_type)| ColumnDefPlan {
                            name: format!("column{}", i + 1),
                            data_type,
                            nullable: true,
                            default: None,
                            unique: None,
                            comment: None,
                        })
                        .collect::<Vec<_>>();

                    Some(column_defs)
                }
            }
        }
    };

    if let Some(column_defs) = target_columns_defs.as_deref() {
        validate_column_names(column_defs)?;

        for column_def in column_defs {
            validate(column_def)?;
        }
    }

    for foreign_key in foreign_keys {
        let ForeignKey {
            referencing_column_name,
            referenced_table_name,
            referenced_column_name,
            ..
        } = foreign_key;

        let referenced_schema = if referenced_table_name == target_table_name {
            None
        } else {
            Some(
                storage
                    .fetch_schema(referenced_table_name)?
                    .ok_or_else(|| {
                        AlterError::ReferencedTableNotFound(referenced_table_name.to_owned())
                    })?,
            )
        };

        let referenced_column = match referenced_schema.as_ref() {
            Some(schema) => schema
                .column_defs
                .as_deref()
                .and_then(|column_defs| {
                    column_defs
                        .iter()
                        .find(|column_def| column_def.name == *referenced_column_name)
                })
                .map(|column_def| (&column_def.data_type, column_def.unique)),
            None => target_columns_defs.as_deref().and_then(|column_defs| {
                column_defs
                    .iter()
                    .find(|column_def| column_def.name == *referenced_column_name)
                    .map(|column_def| (&column_def.data_type, column_def.unique))
            }),
        }
        .ok_or_else(|| AlterError::ReferencedColumnNotFound(referenced_column_name.to_owned()))?;
        let (referenced_data_type, referenced_unique) = referenced_column;

        let referencing_column_def = target_columns_defs
            .as_deref()
            .and_then(|column_defs| {
                column_defs
                    .iter()
                    .find(|column_def| column_def.name == *referencing_column_name)
            })
            .ok_or_else(|| {
                AlterError::ReferencingColumnNotFound(referencing_column_name.to_owned())
            })?;

        if &referencing_column_def.data_type != referenced_data_type {
            return Err(AlterError::ForeignKeyDataTypeMismatch {
                referencing_column: referencing_column_name.to_owned(),
                referencing_column_type: referencing_column_def.data_type.clone(),
                referenced_column: referenced_column_name.to_owned(),
                referenced_column_type: referenced_data_type.clone(),
            }
            .into());
        }

        if referenced_unique != Some(ColumnUniqueOption { is_primary: true }) {
            return Err(AlterError::ReferencingNonPKColumn {
                referenced_table: referenced_table_name.to_owned(),
                referenced_column: referenced_column_name.to_owned(),
            }
            .into());
        }
    }

    if storage.fetch_schema(target_table_name)?.is_none() {
        let schema = Schema {
            table_name: target_table_name.to_owned(),
            column_defs: target_columns_defs.as_ref().map(|column_defs| {
                column_defs
                    .iter()
                    .map(ColumnDefPlan::to_column_def)
                    .collect()
            }),
            indexes: vec![],
            engine: engine.clone(),
            foreign_keys: foreign_keys.clone(),
            comment: comment.clone(),
        };

        storage.insert_schema(&schema)?;
    } else if !if_not_exists {
        return Err(AlterError::TableAlreadyExists(target_table_name.to_owned()).into());
    }

    match source {
        Some(query) => {
            let rows = match selected_source_rows {
                Some(rows) => rows,
                None => query::execute(storage, functions, query, None)?
                    .map(|row| row.map(Row::into_values))
                    .collect::<Result<Vec<_>>>()?,
            };

            storage.append_data(target_table_name, rows)
        }
        None => Ok(()),
    }
}

fn column_defs_from_rows(labels: Vec<String>, rows: &[Vec<Value>]) -> Vec<ColumnDefPlan> {
    labels
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            let data_type = rows
                .iter()
                .filter_map(|row| row.get(index))
                .find_map(Value::get_type)
                .unwrap_or(DataType::Text);

            ColumnDefPlan {
                name,
                data_type,
                nullable: true,
                default: None,
                unique: None,
                comment: None,
            }
        })
        .collect()
}

pub fn drop_table<T: GStore + GStoreMut>(
    storage: &mut T,
    table_names: &[String],
    if_exists: bool,
    cascade: bool,
) -> Result<usize> {
    if !if_exists {
        // read-only pass: nothing is dropped unless every name can be dropped
        let mut scheduled = HashSet::new();

        for table_name in table_names {
            if !scheduled.insert(table_name.as_str()) || storage.fetch_schema(table_name)?.is_none()
            {
                return Err(AlterError::TableNotFound(table_name.to_owned()).into());
            }
        }
    }

    let mut n = 0;

    for table_name in table_names {
        let schema = storage.fetch_schema(table_name)?;

        match (schema, if_exists) {
            (None, true) => {
                continue;
            }
            (None, false) => {
                return Err(AlterError::TableNotFound(table_name.to_owned()).into());
            }
            _ => {}
        }

        let referencings = storage.fetch_referencings(table_name)?;

        if !referencings.is_empty() && !cascade {
            return Err(AlterError::CannotDropTableWithReferencing {
                referenced_table_name: table_name.into(),
                referencings,
            }
            .into());
        }

        for Referencing {
            table_name,
            foreign_key: ForeignKey { name, .. },
        } in referencings
        {
            let mut schema = storage
                .fetch_schema(&table_name)?
                .ok_or_else(|| AlterError::TableNotFound(table_name.clone()))?;
            schema
                .foreign_keys
                .retain(|foreign_key| foreign_key.name != name);
            storage.insert_schema(&schema)?;
        }
        storage.delete_schema(table_name)?;

        n += 1;
    }

    Ok(n)
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Referencing {
    pub table_name: String,
    pub foreign_key: ForeignKey,
}

impl fmt::Display for Referencing {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            r#"{} on table "{}""#,
            self.foreign_key.to_sql(),
            self.table_name
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            ast::ReferentialAction,
            mock::{MockStorage, run},
            result::Error,
        },
    };

    #[test]
    fn unplanned_columns_are_rejected() {
        let mut storage = run("CREATE TABLE Target (id INT)");
        let source = Some(Box::new(QueryPlan::Values(ValuesPlan(Vec::new()))));
        let actual = create_table(
            &mut storage,
            &[],
            CreateTableOptions {
                target_table_name: "Target",
                columns: &CreateTableColumnsPlan::Unplanned { columns: None },
                if_not_exists: true,
                source: &source,
                engine: &None,
                foreign_keys: &Vec::new(),
                comment: &None,
            },
        );
        let expected = Err(Error::Alter(Box::new(
            AlterError::UnplannedCreateTableColumns("Target".to_owned()),
        )));

        assert_eq!(actual, expected);
    }

    #[test]
    fn from_query_requires_source() {
        let mut storage = MockStorage::default();
        let actual = create_table(
            &mut storage,
            &[],
            CreateTableOptions {
                target_table_name: "Target",
                columns: &CreateTableColumnsPlan::FromQuery,
                if_not_exists: false,
                source: &None,
                engine: &None,
                foreign_keys: &Vec::new(),
                comment: &None,
            },
        );
        let expected = Err(Error::Alter(Box::new(
            AlterError::MalformedPlanCreateTableSource,
        )));

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_referencing_display() {
        let referencing = Referencing {
            table_name: "Referencing".to_owned(),
            foreign_key: ForeignKey {
                name: "FK_referenced_id-Referenced_id".to_owned(),
                referencing_column_name: "referenced_id".to_owned(),
                referenced_table_name: "Referenced".to_owned(),
                referenced_column_name: "id".to_owned(),
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            },
        };

        assert_eq!(
            format!("{referencing}"),
            r#"CONSTRAINT "FK_referenced_id-Referenced_id" FOREIGN KEY ("referenced_id") REFERENCES "Referenced" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION on table "Referencing""#
        );
    }
}
