use {
    im_rc::HashSet,
    std::{
        convert::TryInto,
        fmt::Debug,
    },
    crate::{
        data::{
            Row,
            Value
        },
        result::Result,
        utils::Vector,
    },
    super::{
        ValidateError,
        UniqueKey,
    },
};

#[derive(Debug, Clone)]
pub enum Constraint {
    UniqueConstraint {
        column_index: usize,
        column_name: String,
        keys: HashSet<UniqueKey>,
    },
    TypeConstraint {
        column_index: usize,
        column_name: String,
    },
}

impl Constraint {
    pub fn new(constraint: Constraint, column_index: usize, column_name: String) -> Self {
        match constraint {
            Constraint::UniqueConstraint {..} => Constraint::UniqueConstraint {
                column_index,
                column_name,
                keys: HashSet::new(),
            },
            Constraint::TypeConstraint {..} => Constraint::TypeConstraint {
                column_index,
                column_name,
            },
        }
    }

    pub fn add(self, value: &Value) -> Result<Self> {
        Ok(match self {
            Constraint::UniqueConstraint {ref column_index, ref column_name, ref keys} => {
                let new_key = self.check(value)?;
                if new_key == UniqueKey::Null {self}else{
                    let column_index = column_index.to_owned();
                    let column_name = column_name.to_owned();
                    let keys = keys.update(new_key);
                    Constraint::UniqueConstraint {
                        column_index,
                        column_name,
                        keys
                    }
                    
                }
            },
            _ => self,
        })
    }

    pub fn check(&self, value: &Value) -> Result<UniqueKey> {
        Ok(match self {
            Constraint::UniqueConstraint {column_name, keys, ..} => {
                let new_key = value.try_into()?;
                if new_key != UniqueKey::Null && keys.contains(&new_key) {
                    // The input values are duplicate.
                    return Err(ValidateError::DuplicateEntryOnUniqueField(
                        format!("{:?}", value),
                        column_name.to_owned()
                    )
                    .into());
                }
                new_key
            },
            _ => UniqueKey::Null
        })
    }
}

pub fn create_constraints<'a>(
    columns: Vec<(usize, String)>,
    row_iter: impl Iterator<Item = &'a Row> + Clone,
    constraint: Constraint,
) -> Result<Vector<Constraint>> {
    columns
        .into_iter()
        .try_fold(Vector::new(), |constraints, col| {
            let (col_idx, col_name) = col;
            let new_constraint = Constraint::new(constraint.clone(), col_idx, col_name);
            let new_constraint = row_iter
                .clone()
                .try_fold(new_constraint, |constraint, row| {
                    println!("constraint: {:?}, row: {:?}", constraint, row);
                    let val = row
                        .get_value(col_idx)
                        .ok_or(ValidateError::ConflictOnStorageColumnIndex(col_idx))?;
                    constraint.add(val)
                })?;
            Ok(constraints.push(new_constraint))
        })
}
