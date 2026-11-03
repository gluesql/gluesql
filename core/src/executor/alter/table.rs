use {
    super::{AlterError, validate, validate_column_names},
    crate::{
        ast::{ColumnUniqueOption, ForeignKey, ToSql},
        data::{Key, Row, Schema},
        executor::{
            query,
            validate::{validate_foreign_key, validate_row, validate_unique_rows},
        },
        plan::{ColumnDefPlan, CustomFunctionPlan, QueryPlan, TableColumnsPlan},
        result::Result,
        store::{GStore, GStoreMut},
    },
    serde::Serialize,
    std::fmt,
};

pub struct CreateTableOptions<'a> {
    pub target_table_name: &'a str,
    pub columns: &'a TableColumnsPlan,
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
    let target_columns_defs = match columns {
        TableColumnsPlan::Unplanned => return Err(AlterError::UnplannedTableColumns.into()),
        TableColumnsPlan::Schemaless => None,
        TableColumnsPlan::Columns(columns) => Some(columns.as_slice()),
    };
    if source.is_some() && storage.fetch_schema(target_table_name)?.is_some() {
        return if if_not_exists {
            Ok(())
        } else {
            Err(AlterError::TableAlreadyExists(target_table_name.to_owned()).into())
        };
    }

    if let Some(column_defs) = target_columns_defs {
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
            None => target_columns_defs.and_then(|column_defs| {
                column_defs
                    .iter()
                    .find(|column_def| column_def.name == *referenced_column_name)
                    .map(|column_def| (&column_def.data_type, column_def.unique))
            }),
        }
        .ok_or_else(|| AlterError::ReferencedColumnNotFound(referenced_column_name.to_owned()))?;
        let (referenced_data_type, referenced_unique) = referenced_column;

        let referencing_column_def = target_columns_defs
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

    let schema = Schema {
        table_name: target_table_name.to_owned(),
        column_defs: target_columns_defs.map(|column_defs| {
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
    let Some(source) = source.as_deref() else {
        if storage.fetch_schema(target_table_name)?.is_some() {
            return if if_not_exists {
                Ok(())
            } else {
                Err(AlterError::TableAlreadyExists(target_table_name.to_owned()).into())
            };
        }

        return storage.insert_schema(&schema);
    };

    let rows = query::execute(storage, functions, source, None)?
        .map(|row| row.map(Row::into_values))
        .collect::<Result<Vec<_>>>()?;
    for row in &rows {
        validate_row(target_columns_defs, row)?;
    }

    let primary_key = match target_columns_defs {
        Some(columns) => {
            validate_unique_rows(columns, &rows)?;
            validate_foreign_key(storage, columns, foreign_keys, &rows)?;

            columns.iter().position(|column_def| {
                column_def.unique == Some(ColumnUniqueOption { is_primary: true })
            })
        }
        None => None,
    };

    if let Some(index) = primary_key {
        let rows = rows
            .into_iter()
            .map(|row| Key::try_from(&row[index]).map(|key| (key, row)))
            .collect::<Result<Vec<_>>>()?;

        storage.insert_schema(&schema)?;
        storage.insert_data(target_table_name, rows)
    } else {
        storage.insert_schema(&schema)?;
        storage.append_data(target_table_name, rows)
    }
}

pub fn drop_table<T: GStore + GStoreMut>(
    storage: &mut T,
    table_names: &[String],
    if_exists: bool,
    cascade: bool,
) -> Result<usize> {
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
    use {super::*, crate::ast::ReferentialAction};

    #[test]
    fn unplanned_columns_before_existing_target() {
        let mut storage = crate::mock::run("CREATE TABLE Item (id INT)");
        let actual = create_table(
            &mut storage,
            &[],
            CreateTableOptions {
                target_table_name: "Item",
                columns: &TableColumnsPlan::Unplanned,
                if_not_exists: true,
                source: &Some(Box::new(QueryPlan::Values(crate::plan::ValuesPlan(
                    Vec::new(),
                )))),
                engine: &None,
                foreign_keys: &Vec::new(),
                comment: &None,
            },
        );
        assert_eq!(actual, Err(AlterError::UnplannedTableColumns.into()));
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
