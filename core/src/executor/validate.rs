use {
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, ForeignKey},
        data::{Key, Value},
        plan::ColumnDefPlan,
        result::Result,
        store::Store,
    },
    im::HashSet,
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug, PartialEq, Serialize)]
pub enum ValidateError {
    #[error("row has {found} values, expected {expected}")]
    RowLengthMismatch { expected: usize, found: usize },

    #[error("schemaless row must contain a single map value")]
    SchemalessRowRequired,

    #[error(
        "cannot find referenced value on {table_name}.{column_name} with value {referenced_value:?}"
    )]
    CannotFindReferencedValue {
        table_name: String,
        column_name: String,
        referenced_value: String,
    },

    #[error("referencing column not found: {0}")]
    ReferencingColumnNotFound(String),

    #[error("input row has no column on index {0}")]
    RowColumnIndexNotFound(usize),

    #[error("conflict! storage row has no column on index {0}")]
    ConflictOnStorageColumnIndex(usize),

    #[error("duplicate entry '{}' for unique column '{1}'", String::from(.0))]
    DuplicateEntryOnUniqueField(Value, String),

    #[error("duplicate entry '{0:?}' for primary_key field")]
    DuplicateEntryOnPrimaryKeyField(Key),
}

pub enum ColumnValidation<'column_def> {
    /// `INSERT`
    All(&'column_def [ColumnDefPlan]),
    /// `UPDATE`
    SpecifiedColumns(&'column_def [ColumnDef], Vec<String>),
}

#[derive(Debug)]
struct UniqueConstraint {
    column_index: usize,
    column_name: String,
    keys: HashSet<Key>,
}

impl UniqueConstraint {
    fn new(column_index: usize, column_name: String) -> Self {
        Self {
            column_index,
            column_name,
            keys: HashSet::new(),
        }
    }

    fn add(self, value: &Value) -> Result<Self> {
        let new_key = self.check(value)?;

        if matches!(new_key, Key::None) {
            return Ok(self);
        }

        let keys = self.keys.update(new_key);

        Ok(Self {
            column_index: self.column_index,
            column_name: self.column_name,
            keys,
        })
    }

    fn check(&self, value: &Value) -> Result<Key> {
        let key = Key::try_from(value)?;

        if self.keys.contains(&key) {
            Err(
                ValidateError::DuplicateEntryOnUniqueField(value.clone(), self.column_name.clone())
                    .into(),
            )
        } else {
            Ok(key)
        }
    }
}

pub fn validate_unique<'a, T: Store>(
    storage: &T,
    table_name: &str,
    column_validation: &ColumnValidation<'_>,
    row_iter: impl Iterator<Item = &'a [Value]> + Clone,
) -> Result<()> {
    enum Columns {
        /// key index
        PrimaryKeyOnly(usize),
        /// `[(key_index, table_name)]`
        All(Vec<(usize, String)>),
    }

    let columns = match &column_validation {
        ColumnValidation::All(column_defs) => {
            let primary_key_index = column_defs
                .iter()
                .enumerate()
                .find(|(_, ColumnDefPlan { unique, .. })| {
                    unique == &Some(ColumnUniqueOption { is_primary: true })
                })
                .map(|(i, _)| i);
            let other_unique_column_def_count = column_defs
                .iter()
                .filter(|ColumnDefPlan { unique, .. }| {
                    unique == &Some(ColumnUniqueOption { is_primary: false })
                })
                .count();

            match (primary_key_index, other_unique_column_def_count) {
                (Some(primary_key_index), 0) => Columns::PrimaryKeyOnly(primary_key_index),
                _ => Columns::All(fetch_all_unique_columns(column_defs)),
            }
        }
        ColumnValidation::SpecifiedColumns(column_defs, specified_columns) => Columns::All(
            fetch_specified_unique_columns(column_defs, specified_columns),
        ),
    };

    match columns {
        Columns::PrimaryKeyOnly(primary_key_index) => {
            for primary_key in
                row_iter.filter_map(|row| row.get(primary_key_index).map(Key::try_from))
            {
                let key = primary_key?;

                if storage.fetch_data(table_name, &key)?.is_some() {
                    return Err(ValidateError::DuplicateEntryOnPrimaryKeyField(key).into());
                }
            }

            Ok(())
        }
        Columns::All(columns) => {
            let unique_constraints = create_unique_constraints(columns, &row_iter)?;
            if unique_constraints.is_empty() {
                return Ok(());
            }

            for row in storage.scan_data(table_name)? {
                let (_, values) = row?;
                for constraint in &unique_constraints {
                    let col_idx = constraint.column_index;
                    let val = values
                        .get(col_idx)
                        .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;

                    constraint.check(val)?;
                }
            }

            Ok(())
        }
    }
}

pub fn validate_foreign_key<T: Store>(
    storage: &T,
    column_defs: &[ColumnDefPlan],
    foreign_keys: &[ForeignKey],
    rows: &[Vec<Value>],
) -> Result<()> {
    for foreign_key in foreign_keys {
        let ForeignKey {
            referencing_column_name,
            referenced_table_name,
            referenced_column_name,
            ..
        } = foreign_key;

        let target_index = column_defs
            .iter()
            .enumerate()
            .find(|(_, c)| &c.name == referencing_column_name)
            .ok_or_else(|| {
                ValidateError::ReferencingColumnNotFound(referencing_column_name.to_owned())
            })?;

        for row in rows {
            let value = row
                .get(target_index.0)
                .ok_or(ValidateError::RowColumnIndexNotFound(target_index.0))?;

            if value == &Value::Null {
                continue;
            }

            let no_referenced = storage
                .fetch_data(referenced_table_name, &Key::try_from(value)?)?
                .is_none();

            if no_referenced {
                return Err(ValidateError::CannotFindReferencedValue {
                    table_name: referenced_table_name.to_owned(),
                    column_name: referenced_column_name.to_owned(),
                    referenced_value: String::from(value),
                }
                .into());
            }
        }
    }

    Ok(())
}

pub fn validate_row(column_defs: Option<&[ColumnDefPlan]>, row: &[Value]) -> Result<()> {
    let Some(column_defs) = column_defs else {
        return if matches!(row, [Value::Map(_)]) {
            Ok(())
        } else {
            Err(ValidateError::SchemalessRowRequired.into())
        };
    };
    if row.len() != column_defs.len() {
        return Err(ValidateError::RowLengthMismatch {
            expected: column_defs.len(),
            found: row.len(),
        }
        .into());
    }
    for (column, value) in column_defs.iter().zip(row) {
        value.validate_type(&column.data_type)?;
        value.validate_null(column.nullable)?;
    }
    Ok(())
}

pub fn validate_unique_rows(column_defs: &[ColumnDefPlan], rows: &[Vec<Value>]) -> Result<()> {
    create_unique_constraints(
        fetch_all_unique_columns(column_defs),
        &rows.iter().map(Vec::as_slice),
    )
    .map(|_| ())
}

fn create_unique_constraints<'a>(
    unique_columns: Vec<(usize, String)>,
    row_iter: &(impl Iterator<Item = &'a [Value]> + Clone),
) -> Result<Vec<UniqueConstraint>> {
    let mut constraints = Vec::with_capacity(unique_columns.len());

    for (col_idx, col_name) in unique_columns {
        let new_constraint = UniqueConstraint::new(col_idx, col_name);
        let new_constraint = row_iter
            .clone()
            .try_fold(new_constraint, |constraint, row| {
                let val = row
                    .get(col_idx)
                    .ok_or(ValidateError::RowColumnIndexNotFound(col_idx))?;

                constraint.add(val)
            })?;

        constraints.push(new_constraint);
    }

    Ok(constraints)
}

fn fetch_all_unique_columns(column_defs: &[ColumnDefPlan]) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| table_col.unique.map(|_| (i, table_col.name.clone())))
        .collect()
}

fn fetch_specified_unique_columns(
    all_column_defs: &[ColumnDef],
    specified_columns: &[String],
) -> Vec<(usize, String)> {
    all_column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            (table_col.unique.is_some()
                && specified_columns.iter().any(|col| col == &table_col.name))
            .then_some((i, table_col.name.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::{ValidateError, validate_foreign_key, validate_row, validate_unique_rows},
        crate::{
            data::{Key, Value, ValueError},
            mock::{MockStorage, run},
            plan::ColumnDefPlan,
            result::Error,
            store::{Store, StoreMut},
        },
        std::collections::BTreeMap,
    };

    #[test]
    fn row() {
        let storage = run("CREATE TABLE Item (id INT NOT NULL, label TEXT)");
        let columns: Vec<_> = storage
            .fetch_schema("Item")
            .unwrap()
            .unwrap()
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect();
        assert_eq!(
            validate_row(Some(&columns), &[Value::I64(1), Value::Null]),
            Ok(()),
            "nullable value"
        );
        assert_eq!(
            validate_row(Some(&columns), &[Value::I64(1)]),
            Err(Error::Validate(ValidateError::RowLengthMismatch {
                expected: 2,
                found: 1
            })),
            "missing value"
        );
        assert_eq!(
            validate_row(Some(&columns), &[Value::I64(1), Value::Null, Value::Null]),
            Err(Error::Validate(ValidateError::RowLengthMismatch {
                expected: 2,
                found: 3
            })),
            "excess value"
        );
        assert_eq!(
            validate_row(Some(&columns), &[Value::Null, Value::Null]),
            Err(ValueError::NullValueOnNotNullField.into()),
            "required value"
        );
        assert_eq!(
            validate_row(Some(&columns), &[Value::F64(1.0), Value::Null]),
            Err(ValueError::IncompatibleDataType {
                data_type: crate::ast::DataType::Int,
                value: Value::F64(1.0)
            }
            .into()),
            "typed value mismatch"
        );
        assert_eq!(
            validate_row(None, &[Value::Map(BTreeMap::new())]),
            Ok(()),
            "schemaless document"
        );
        assert_eq!(
            validate_row(None, &[Value::Null]),
            Err(Error::Validate(ValidateError::SchemalessRowRequired))
        );
        assert_eq!(
            validate_row(None, &[]),
            Err(Error::Validate(ValidateError::SchemalessRowRequired))
        );
    }

    #[test]
    fn unique_rows() {
        let storage = run("CREATE TABLE Item (id INT PRIMARY KEY, label TEXT UNIQUE)");
        let columns: Vec<_> = storage
            .fetch_schema("Item")
            .unwrap()
            .unwrap()
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect();
        let rows = vec![
            vec![Value::I64(1), Value::Null],
            vec![Value::I64(2), Value::Null],
        ];
        assert_eq!(
            validate_unique_rows(&columns, &rows),
            Ok(()),
            "nulls do not conflict"
        );
        let rows = vec![
            vec![Value::I64(1), Value::Null],
            vec![Value::I64(1), Value::Null],
        ];
        let expected = Err(Error::Validate(ValidateError::DuplicateEntryOnUniqueField(
            Value::I64(1),
            "id".to_owned(),
        )));
        assert_eq!(
            validate_unique_rows(&columns, &rows),
            expected,
            "duplicate new primary keys"
        );
        let rows = vec![
            vec![Value::I64(1), Value::Str("same".to_owned())],
            vec![Value::I64(2), Value::Str("same".to_owned())],
        ];
        let expected = Err(Error::Validate(ValidateError::DuplicateEntryOnUniqueField(
            Value::Str("same".to_owned()),
            "label".to_owned(),
        )));
        assert_eq!(
            validate_unique_rows(&columns, &rows),
            expected,
            "duplicate new unique values"
        );
        assert_eq!(
            validate_unique_rows(&[], &rows),
            Ok(()),
            "no unique columns"
        );
        assert_eq!(
            validate_unique_rows(&columns, &[Vec::new()]),
            Err(Error::Validate(ValidateError::RowColumnIndexNotFound(0))),
            "invalid input row is not a storage conflict"
        );
    }

    #[test]
    fn foreign_key() {
        let mut storage = run("
            CREATE TABLE Parent (id INT PRIMARY KEY);
            CREATE TABLE Child (parent INT, FOREIGN KEY (parent) REFERENCES Parent(id));
        ");
        let schema = storage.fetch_schema("Child").unwrap().unwrap();
        let columns: Vec<_> = schema
            .column_defs
            .unwrap()
            .into_iter()
            .map(ColumnDefPlan::from)
            .collect();
        let keys = schema.foreign_keys;
        assert_eq!(
            validate_foreign_key(&storage, &columns, &[], &[vec![Value::I64(1)]]),
            Ok(()),
            "no foreign keys"
        );
        assert_eq!(
            validate_foreign_key(&storage, &columns, &keys, &[vec![Value::Null]]),
            Ok(()),
            "NULL reference"
        );
        let expected = Err(Error::Validate(ValidateError::CannotFindReferencedValue {
            table_name: "Parent".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "1".to_owned(),
        }));
        assert_eq!(
            validate_foreign_key(&storage, &columns, &keys, &[vec![Value::I64(1)]]),
            expected,
            "missing referenced key"
        );
        storage
            .insert_data("Parent", vec![(Key::I64(1), vec![Value::I64(1)])])
            .unwrap();
        assert_eq!(
            validate_foreign_key(&storage, &columns, &keys, &[vec![Value::I64(1)]]),
            Ok(()),
            "existing referenced key"
        );
        assert_eq!(
            validate_foreign_key(&storage, &[], &keys, &[]),
            Err(Error::Validate(ValidateError::ReferencingColumnNotFound(
                "parent".to_owned()
            )))
        );
        assert_eq!(
            validate_foreign_key(&storage, &columns, &keys, &[vec![]]),
            Err(Error::Validate(ValidateError::RowColumnIndexNotFound(0)))
        );

        let empty = MockStorage::default();
        assert_eq!(
            validate_foreign_key(&empty, &columns, &keys, &[]),
            Ok(()),
            "empty input needs no referenced rows"
        );
    }
}
