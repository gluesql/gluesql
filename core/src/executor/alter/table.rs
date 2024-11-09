use {
    super::{validate, validate_column_names, AlterError},
    crate::{
        ast::{
            ColumnDef, ColumnUniqueOption, ForeignKey, Query, SetExpr, TableFactor, ToSql, Values,
        },
        data::{Schema, TableError},
        executor::{evaluate_stateless, select::select},
        prelude::{DataType, Value},
        result::{Error, Result},
        store::{GStore, GStoreMut},
    },
    futures::stream::TryStreamExt,
    serde::Serialize,
    std::fmt,
};

pub struct CreateTableOptions<'a> {
    pub target_table_name: &'a str,
    pub column_defs: Option<&'a [ColumnDef]>,
    pub if_not_exists: bool,
    pub source: &'a Option<Box<Query>>,
    pub engine: &'a Option<String>,
    pub foreign_keys: &'a Vec<ForeignKey>,
    pub comment: &'a Option<String>,
}

pub async fn create_table<T: GStore + GStoreMut>(
    storage: &mut T,
    CreateTableOptions {
        target_table_name,
        column_defs,
        if_not_exists,
        source,
        engine,
        foreign_keys,
        comment,
    }: CreateTableOptions<'_>,
) -> Result<()> {
    let target_columns_defs = match source.as_deref() {
        Some(Query { body, .. }) => match body {
            SetExpr::Select(select_query) => match &select_query.from.relation {
                TableFactor::Table { name, .. } => {
                    let schema = storage.fetch_schema(name).await?;
                    let Schema {
                        column_defs: source_column_defs,
                        ..
                    } = schema
                        .ok_or_else(|| AlterError::CtasSourceTableNotFound(name.to_owned()))?;

                    source_column_defs
                }
                TableFactor::Series { .. } => {
                    let column_def = ColumnDef {
                        name: "N".into(),
                        data_type: DataType::Int,
                        nullable: false,
                        default: None,
                        unique: None,
                        comment: None,
                    };

                    Some(vec![column_def])
                }
                _ => {
                    return Err(Error::Table(TableError::Unreachable));
                }
            },
            SetExpr::Values(Values(values_list)) => {
                let first_len = values_list[0].len();
                let mut column_types = vec![None; first_len];

                for exprs in values_list {
                    for (i, expr) in exprs.iter().enumerate() {
                        if column_types[i].is_some() {
                            continue;
                        }

                        column_types[i] = evaluate_stateless(None, expr)
                            .await
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
                    .map(|(i, data_type)| ColumnDef {
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
        },
        None if column_defs.is_some() => column_defs.map(<[ColumnDef]>::to_vec),
        None => None,
    };

    if let Some(column_defs) = target_columns_defs.as_deref() {
        validate_column_names(column_defs)?;

        for column_def in column_defs {
            validate(column_def).await?;
        }
    }

    for foreign_key in foreign_keys {
        let ForeignKey {
            referencing_column_name,
            referenced_table_name,
            referenced_column_name,
            ..
        } = foreign_key;

        let column_defs = if referenced_table_name == target_table_name {
            target_columns_defs.clone()
        } else {
            let referenced_schema = storage
                .fetch_schema(referenced_table_name)
                .await?
                .ok_or_else(|| {
                    AlterError::ReferencedTableNotFound(referenced_table_name.to_owned())
                })?;

            referenced_schema.column_defs
        };

        let referenced_column_def = column_defs
            .and_then(|column_defs| {
                column_defs
                    .into_iter()
                    .find(|column_def| column_def.name == *referenced_column_name)
            })
            .ok_or_else(|| AlterError::ReferencedColumnNotFound(referenced_column_name.to_owned()))?
            .to_owned();

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

        if referencing_column_def.data_type != referenced_column_def.data_type {
            return Err(AlterError::ForeignKeyDataTypeMismatch {
                referencing_column: referencing_column_name.to_owned(),
                referencing_column_type: referencing_column_def.data_type.to_owned(),
                referenced_column: referenced_column_name.to_owned(),
                referenced_column_type: referenced_column_def.data_type.to_owned(),
            }
            .into());
        }

        if referenced_column_def.unique != Some(ColumnUniqueOption { is_primary: true }) {
            return Err(AlterError::ReferencingNonPKColumn {
                referenced_table: referenced_table_name.to_owned(),
                referenced_column: referenced_column_name.to_owned(),
            }
            .into());
        }
    }

    if storage.fetch_schema(target_table_name).await?.is_none() {
        let schema = Schema {
            table_name: target_table_name.to_owned(),
            column_defs: target_columns_defs,
            indexes: vec![],
            engine: engine.clone(),
            foreign_keys: foreign_keys.clone(),
            comment: comment.clone(),
        };

        storage.insert_schema(&schema).await?;
    } else if !if_not_exists {
        return Err(AlterError::TableAlreadyExists(target_table_name.to_owned()).into());
    }

    match source {
        Some(query) => {
            let rows = select(storage, query, None)
                .await?
                .map_ok(Into::into)
                .try_collect()
                .await?;

            storage
                .append_data(target_table_name, rows)
                .await
                .map(|_| ())
        }
        None => Ok(()),
    }
}

pub async fn drop_table<T: GStore + GStoreMut>(
    storage: &mut T,
    table_names: &[String],
    if_exists: bool,
    cascade: bool,
) -> Result<usize> {
    let mut n = 0;

    for table_name in table_names {
        let schema = storage.fetch_schema(table_name).await?;

        match (schema, if_exists) {
            (None, true) => {
                continue;
            }
            (None, false) => {
                return Err(AlterError::TableNotFound(table_name.to_owned()).into());
            }
            _ => {}
        }

        let referencings = storage.fetch_referencings(table_name).await?;

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
                .fetch_schema(&table_name)
                .await?
                .ok_or_else(|| AlterError::TableNotFound(table_name.to_owned()))?;
            schema
                .foreign_keys
                .retain(|foreign_key| foreign_key.name != name);
            storage.insert_schema(&schema).await?;
        }
        storage.delete_schema(table_name).await?;

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
            format!("{}", referencing),
            r#"CONSTRAINT "FK_referenced_id-Referenced_id" FOREIGN KEY ("referenced_id") REFERENCES "Referenced" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION on table "Referencing""#
        );
    }
}
