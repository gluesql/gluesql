//! `INSERT ... ON CONFLICT`: what becomes of a row whose unique or primary key value is
//! already in the table.
//!
//! The clause names the constraint to watch — the conflict target — and what to do when
//! a row hits it: leave the table as it is (`DO NOTHING`), or update the row that is
//! already there (`DO UPDATE SET ...`, whose assignments see the proposed row under the
//! `excluded` alias). Rows are taken in order, as `PostgreSQL` takes them, so a row
//! conflicting with one accepted earlier in the same statement conflicts exactly as it
//! would with a row already stored.
//!
//! `GlueSQL` has no composite unique constraint, so a conflict target is one column, and
//! it must be that table's primary key or a `UNIQUE` column. Watching the primary key
//! alone is a key lookup; anything else costs one scan of the table, which is what
//! validating uniqueness costs anyway.

use {
    super::InsertError,
    crate::{
        ast::{ColumnDef, ColumnUniqueOption, ForeignKey},
        data::{Key, Row, Value},
        executor::{
            filter::check_expr,
            update::{Update, conflict_context},
            validate::ValidateError,
        },
        plan::{OnConflictActionPlan, OnConflictPlan},
        result::Result,
        store::GStore,
    },
    std::{
        collections::{HashMap, HashSet},
        rc::Rc,
    },
};

/// The stored row a proposed row conflicts with, if any, one per proposed row.
type Conflicts = Vec<Option<(Key, Vec<Value>)>>;

/// What the statement turned out to be: rows to add, and rows already in the table to
/// overwrite.
pub(super) struct Resolved {
    pub rows: Vec<Vec<Value>>,
    pub updated: Vec<(Key, Vec<Value>)>,
}

/// Splits the proposed rows into the ones to insert and the ones that conflict, applying
/// the clause to each conflict.
pub(super) fn resolve<T: GStore>(
    storage: &T,
    table_name: &str,
    column_defs: &[ColumnDef],
    foreign_keys: &[ForeignKey],
    on_conflict: &OnConflictPlan,
    rows: Vec<Vec<Value>>,
) -> Result<Resolved> {
    let targets = conflict_columns(column_defs, on_conflict)?;
    let primary_key = column_defs
        .iter()
        .position(|column_def| column_def.unique == Some(ColumnUniqueOption { is_primary: true }));
    let stored = lookup(storage, table_name, &targets, primary_key, &rows)?;

    let columns: Rc<[String]> = column_defs
        .iter()
        .map(|column_def| column_def.name.clone())
        .collect();
    let (update, selection) = match &on_conflict.action {
        OnConflictActionPlan::DoNothing => (None, None),
        OnConflictActionPlan::DoUpdate {
            assignments,
            selection,
        } => (
            Some(Update::new(
                storage,
                table_name,
                assignments,
                Some(column_defs),
            )?),
            selection.as_ref(),
        ),
    };

    let mut kept: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    let mut updated: Vec<(Key, Vec<Value>)> = Vec::new();
    // What the rows taken so far in this statement occupy, and which stored rows they
    // have already overwritten.
    let mut taken: HashSet<(usize, Key)> = HashSet::new();
    let mut overwritten: HashSet<Key> = HashSet::new();

    for (values, stored) in rows.into_iter().zip(stored) {
        let in_batch = conflicts_with_taken(&targets, &values, &taken)?;

        if stored.is_none() && !in_batch {
            remember(&targets, &values, &mut taken)?;
            kept.push(values);
            continue;
        }

        let Some(update) = update.as_ref() else {
            // DO NOTHING: the table keeps what it has.
            continue;
        };

        // DO UPDATE needs a row to update, and each row may be updated once. A row
        // conflicting only with one proposed earlier in the same statement has no stored
        // row behind it, which PostgreSQL rejects in the same terms.
        let Some((key, present)) = stored else {
            return Err(InsertError::ConflictAffectsRowTwice.into());
        };
        if !overwritten.insert(key.clone()) {
            return Err(InsertError::ConflictAffectsRowTwice.into());
        }

        let present = Row {
            columns: Rc::clone(&columns),
            values: present,
        };
        let proposed = Row {
            columns: Rc::clone(&columns),
            values,
        };

        if let Some(selection) = selection {
            let context = Rc::new(conflict_context(table_name, &present, &proposed));

            if !check_expr(storage, Some(&context), None, selection)? {
                continue;
            }
        }

        let row = update.apply_with(present, foreign_keys, Some(&proposed))?;
        updated.push((key, row.into_values()));
    }

    validate_updated_unique(storage, table_name, column_defs, &kept, &updated)?;

    Ok(Resolved {
        rows: kept,
        updated,
    })
}

/// A schemaless table has no constraint to conflict on, so only the clause that never
/// fires is accepted — as in `PostgreSQL`, where a target without a matching constraint is
/// an error and a bare `DO NOTHING` is not.
pub(super) fn reject_schemaless(on_conflict: &OnConflictPlan) -> Result<()> {
    match (&on_conflict.target, &on_conflict.action) {
        (None, OnConflictActionPlan::DoNothing) => Ok(()),
        (target, _) => Err(InsertError::NoUniqueConstraintForTarget(
            target
                .as_ref()
                .map_or_else(String::new, |target| target.join(", ")),
        )
        .into()),
    }
}

/// The columns a conflict is watched on: the target the statement named, or every unique
/// column when it named none.
fn conflict_columns(column_defs: &[ColumnDef], on_conflict: &OnConflictPlan) -> Result<Vec<usize>> {
    let unique = |column_def: &ColumnDef| column_def.unique.is_some();

    match &on_conflict.target {
        // No target: `DO NOTHING` watches every unique column, and a table with none
        // simply never conflicts. `DO UPDATE` has no row to update without one.
        None => match on_conflict.action {
            OnConflictActionPlan::DoNothing => Ok(column_defs
                .iter()
                .enumerate()
                .filter(|(_, column_def)| unique(column_def))
                .map(|(index, _)| index)
                .collect()),
            OnConflictActionPlan::DoUpdate { .. } => {
                Err(InsertError::ConflictTargetRequired.into())
            }
        },
        Some(target) => {
            let matched = match target.as_slice() {
                [column] => column_defs
                    .iter()
                    .position(|column_def| &column_def.name == column && unique(column_def)),
                // Several columns would name a composite unique constraint, which
                // `GlueSQL` does not have.
                _ => None,
            };

            matched
                .map(|index| vec![index])
                .ok_or_else(|| InsertError::NoUniqueConstraintForTarget(target.join(", ")).into())
        }
    }
}

/// The stored row each proposed row conflicts with, in order.
fn lookup<T: GStore>(
    storage: &T,
    table_name: &str,
    targets: &[usize],
    primary_key: Option<usize>,
    rows: &[Vec<Value>],
) -> Result<Conflicts> {
    if targets.is_empty() || rows.is_empty() {
        return Ok(vec![None; rows.len()]);
    }

    // Watching the primary key alone is a key lookup, which is what having one is for.
    if primary_key.is_some_and(|index| targets == [index]) {
        let index = targets[0];

        return rows
            .iter()
            .map(|values| match key_of(values.get(index))? {
                Some(key) => Ok(storage
                    .fetch_data(table_name, &key)?
                    .map(|stored| (key, stored))),
                None => Ok(None),
            })
            .collect();
    }

    // Otherwise one scan, keeping only the rows the statement could collide with, so the
    // memory this costs follows the statement and not the table.
    let mut wanted: HashSet<(usize, Key)> = HashSet::new();
    for values in rows {
        for &index in targets {
            if let Some(key) = key_of(values.get(index))? {
                wanted.insert((index, key));
            }
        }
    }

    let mut found: HashMap<(usize, Key), (Key, Vec<Value>)> = HashMap::new();
    for row in storage.scan_data(table_name)? {
        let (key, values) = row?;

        for &index in targets {
            let Some(value_key) = key_of(values.get(index))? else {
                continue;
            };

            if wanted.contains(&(index, value_key.clone())) {
                found.insert((index, value_key), (key.clone(), values.clone()));
            }
        }
    }

    rows.iter()
        .map(|values| {
            for &index in targets {
                let Some(value_key) = key_of(values.get(index))? else {
                    continue;
                };

                if let Some(stored) = found.get(&(index, value_key)) {
                    return Ok(Some(stored.clone()));
                }
            }

            Ok(None)
        })
        .collect()
}

/// Whether a row collides with one already taken in this statement.
fn conflicts_with_taken(
    targets: &[usize],
    values: &[Value],
    taken: &HashSet<(usize, Key)>,
) -> Result<bool> {
    for &index in targets {
        if let Some(key) = key_of(values.get(index))?
            && taken.contains(&(index, key))
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn remember(targets: &[usize], values: &[Value], taken: &mut HashSet<(usize, Key)>) -> Result<()> {
    for &index in targets {
        if let Some(key) = key_of(values.get(index))? {
            taken.insert((index, key));
        }
    }

    Ok(())
}

/// A `DO UPDATE` writing a unique column must not collide with another row.
/// [`crate::executor::validate::validate_unique`] cannot answer this: the row being
/// updated is in the table already, so it would be found colliding with itself.
fn validate_updated_unique<T: GStore>(
    storage: &T,
    table_name: &str,
    column_defs: &[ColumnDef],
    inserted: &[Vec<Value>],
    updated: &[(Key, Vec<Value>)],
) -> Result<()> {
    if updated.is_empty() {
        return Ok(());
    }

    let columns: Vec<(usize, &str)> = column_defs
        .iter()
        .enumerate()
        .filter(|(_, column_def)| column_def.unique.is_some())
        .map(|(index, column_def)| (index, column_def.name.as_str()))
        .collect();
    if columns.is_empty() {
        return Ok(());
    }

    // Who claims each unique value after this statement: a row being updated, by its
    // key, or a row being inserted.
    let mut claimed: HashMap<(usize, Key), Option<&Key>> = HashMap::new();

    for (key, values) in updated {
        for &(index, name) in &columns {
            let Some(value_key) = key_of(values.get(index))? else {
                continue;
            };

            if let Some(owner) = claimed.insert((index, value_key), Some(key))
                && owner != Some(key)
            {
                return Err(duplicate(values.get(index), name));
            }
        }
    }

    for values in inserted {
        for &(index, name) in &columns {
            let Some(value_key) = key_of(values.get(index))? else {
                continue;
            };

            if claimed.insert((index, value_key), None).is_some() {
                return Err(duplicate(values.get(index), name));
            }
        }
    }

    for row in storage.scan_data(table_name)? {
        let (key, values) = row?;

        for &(index, name) in &columns {
            let Some(value_key) = key_of(values.get(index))? else {
                continue;
            };

            if claimed
                .get(&(index, value_key))
                .is_some_and(|owner| *owner != Some(&key))
            {
                return Err(duplicate(values.get(index), name));
            }
        }
    }

    Ok(())
}

fn duplicate(value: Option<&Value>, column_name: &str) -> crate::result::Error {
    ValidateError::DuplicateEntryOnUniqueField(
        value.cloned().unwrap_or(Value::Null),
        column_name.to_owned(),
    )
    .into()
}

/// The key a value occupies in a unique column. `NULL` occupies none: it never conflicts.
fn key_of(value: Option<&Value>) -> Result<Option<Key>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => Key::try_from(value).map(Some),
    }
}
