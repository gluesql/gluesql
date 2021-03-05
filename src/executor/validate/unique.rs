use {
    super::{ColumnValidation, ValidateError},
    crate::{
        data::{Row, Value},
        result::Result,
        store::Store,
        utils::Vector,
    },
    im_rc::HashSet,
    sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Ident},
    std::{convert::TryInto, fmt::Debug, rc::Rc},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UniqueKey {
    Bool(bool),
    I64(i64),
    Str(String),
    Null,
}

#[derive(Debug)]
struct UniqueConstraint {
    column_index: usize,
    column_name: String,
    keys: HashSet<UniqueKey>,
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
        if new_key == UniqueKey::Null {
            return Ok(self);
        }

        let keys = self.keys.update(new_key);

        Ok(Self {
            column_index: self.column_index,
            column_name: self.column_name,
            keys,
        })
    }

    fn check(&self, value: &Value) -> Result<UniqueKey> {
        let new_key = value.try_into()?;
        if new_key != UniqueKey::Null && self.keys.contains(&new_key) {
            // The input values are duplicate.
            return Err(ValidateError::DuplicateEntryOnUniqueField(
                format!("{:?}", value),
                self.column_name.to_owned(),
            )
            .into());
        }

        Ok(new_key)
    }
}

pub async fn validate_unique<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let columns = match column_validation {
        ColumnValidation::All(column_defs) => {
            fetch_matches(&column_defs, &|opt_def: &ColumnOptionDef, _| {
                matches!(opt_def.option, ColumnOption::Unique { .. })
            })
        }
        ColumnValidation::SpecifiedColumns(column_defs, specified_columns) => fetch_matches(
            &column_defs,
            &|opt_def: &ColumnOptionDef, table_col: &ColumnDef| match opt_def.option {
                ColumnOption::Unique { .. } => (&specified_columns)
                    .iter()
                    .any(|specified_col| specified_col.value == table_col.name.value),
                _ => false,
            },
        ),
    };

    let unique_constraints: Vec<_> = create_unique_constraints(columns, row_iter)?.into();
    if unique_constraints.is_empty() {
        return Ok(());
    }

    let unique_constraints = Rc::new(unique_constraints);
    storage.scan_data(table_name).await?.try_for_each(|result| {
        let (_, row) = result?;
        Rc::clone(&unique_constraints)
            .iter()
            .try_for_each(|constraint| {
                let col_idx = constraint.column_index;
                let val = row
                    .get_value(col_idx)
                    .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                constraint.check(val)?;
                Ok(())
            })
    })
}

pub async fn validate_increment<T: 'static + Debug>(
    storage: &impl Store<T>,
    table_name: &str,
    column_validation: ColumnValidation,
    row_iter: impl Iterator<Item = &Row> + Clone,
) -> Result<()> {
    let match_name = "AUTO_INCREMENT".to_string();
    let columns = match column_validation {
        ColumnValidation::All(column_defs)
        | ColumnValidation::SpecifiedColumns(column_defs, ..) => {
            fetch_matches(&column_defs, &|opt_def: &ColumnOptionDef, _| {
                matches!(
                    &opt_def.name,
                    Some(Ident {
                        value: match_name,
                        ..
                    })
                )
            })
        }
    };
    for row in row_iter {
        for (index, name) in columns.clone() {
            println!("{:?}", row.get_value(index));
        }
    }
    Ok(())
}

fn create_unique_constraints<'a>(
    unique_columns: Vec<(usize, String)>,
    row_iter: impl Iterator<Item = &'a Row> + Clone,
) -> Result<Vector<UniqueConstraint>> {
    unique_columns
        .into_iter()
        .try_fold(Vector::new(), |constraints, col| {
            let (col_idx, col_name) = col;
            let new_constraint = UniqueConstraint::new(col_idx, col_name);
            let new_constraint = row_iter
                .clone()
                .try_fold(new_constraint, |constraint, row| {
                    let val = row
                        .get_value(col_idx)
                        .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                    constraint.add(val)
                })?;
            Ok(constraints.push(new_constraint))
        })
}

fn fetch_matches(
    column_defs: &[ColumnDef],
    matches: &dyn Fn(&ColumnOptionDef, &ColumnDef) -> bool,
) -> Vec<(usize, String)> {
    column_defs
        .iter()
        .enumerate()
        .filter_map(|(i, table_col)| {
            if table_col
                .options
                .iter()
                .any(|column_defs| matches(column_defs, table_col))
            {
                Some((i, table_col.name.value.to_owned()))
            } else {
                None
            }
        })
        .collect()
}
