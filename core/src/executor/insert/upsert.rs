use {
    super::{InsertError, schemaful},
    crate::{
        ast::{ColumnDef, ForeignKey},
        data::{Key, Row, Value},
        executor::{
            context::RowContext, fetch::fetch, filter::check_expr, update::Update,
            validate::ValidateError,
        },
        plan::{OnConflictActionPlan, OnConflictPlan},
        result::Result,
        store::{GStore, GStoreMut},
    },
    std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        rc::Rc,
    },
};

pub(super) struct UpsertOutcome {
    pub rows_affected: usize,
    /// Final values of every inserted or updated row, in statement order.
    pub affected_rows: Vec<Vec<Value>>,
    /// The rows [`apply`] hands to the storage once the caller is done
    /// evaluating anything that must not observe the mutation.
    pub write: PendingWrite,
}

/// The storage mutation [`execute`] prepared but did not perform.
pub(super) struct PendingWrite {
    keyed: Option<Vec<(Key, Vec<Value>)>>,
    append: Vec<Vec<Value>>,
}

/// Performs the mutation [`execute`] prepared.
///
/// # Errors
///
/// Returns an error when the storage rejects the write.
pub(super) fn apply<T: GStoreMut>(
    storage: &mut T,
    table_name: &str,
    write: PendingWrite,
) -> Result<()> {
    let PendingWrite { keyed, append } = write;

    if let Some(keyed) = keyed {
        storage.insert_data(table_name, keyed)?;
    }
    if !append.is_empty() {
        storage.append_data(table_name, append)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Slot {
    Existing(usize),
    Fresh(usize),
}

/// Computes the outcome of an `INSERT ... ON CONFLICT` without touching the
/// storage. The caller passes [`UpsertOutcome::write`] to [`apply`] once every
/// fallible step that must not observe the mutation has run.
pub(super) fn execute<T: GStore>(
    storage: &T,
    table_name: &str,
    column_defs: &Rc<[ColumnDef]>,
    foreign_keys: &[ForeignKey],
    rows: Vec<Vec<Value>>,
    on_conflict: &OnConflictPlan,
) -> Result<UpsertOutcome> {
    let unique_columns: Vec<(usize, String)> = column_defs
        .iter()
        .enumerate()
        .filter(|(_, column_def)| column_def.unique.is_some())
        .map(|(index, column_def)| (index, column_def.name.clone()))
        .collect();

    let target_columns: Vec<usize> =
        match (on_conflict.conflict_target.as_slice(), &on_conflict.action) {
            ([], OnConflictActionPlan::DoUpdate { .. }) => {
                return Err(InsertError::ConflictTargetRequiredForDoUpdate.into());
            }
            ([], OnConflictActionPlan::DoNothing) => {
                unique_columns.iter().map(|(index, _)| *index).collect()
            }
            ([name], _) => {
                let index = unique_columns
                    .iter()
                    .find(|(_, column_name)| column_name == name)
                    .map(|(index, _)| *index)
                    .ok_or_else(|| InsertError::ConflictTargetNotUnique(name.clone()))?;

                vec![index]
            }
            _ => return Err(InsertError::ConflictTargetMustBeSingleColumn.into()),
        };

    let column_names: Rc<[String]> = column_defs
        .iter()
        .map(|column_def| column_def.name.clone())
        .collect::<Vec<_>>()
        .into();

    let mut existing: Vec<(Key, Vec<Value>)> =
        fetch(storage, table_name, Rc::clone(&column_names), None)?
            .map(|item| item.map(|(key, row)| (key, row.values)))
            .collect::<Result<_>>()?;

    let mut unique_maps: HashMap<usize, HashMap<Key, Slot>> = unique_columns
        .iter()
        .map(|(index, _)| (*index, HashMap::new()))
        .collect();
    for (position, (_, values)) in existing.iter().enumerate() {
        for (index, _) in &unique_columns {
            let key = Key::try_from(&values[*index])?;
            if key != Key::None
                && let Some(map) = unique_maps.get_mut(index)
            {
                map.insert(key, Slot::Existing(position));
            }
        }
    }

    let (assignments, selection) = match &on_conflict.action {
        OnConflictActionPlan::DoNothing => (None, None),
        OnConflictActionPlan::DoUpdate {
            assignments,
            selection,
        } => (Some(assignments), selection.as_ref()),
    };
    let update = assignments
        .map(|assignments| Update::new(storage, table_name, assignments, Some(column_defs)))
        .transpose()?;

    let mut fresh: Vec<Vec<Value>> = Vec::new();
    let mut dirty: HashSet<usize> = HashSet::new();
    let mut affected: Vec<Slot> = Vec::new();
    let mut affected_seen: HashSet<Slot> = HashSet::new();
    let mut rows_affected = 0;

    for row in rows {
        let mut conflict = None;
        for index in &target_columns {
            let key = Key::try_from(&row[*index])?;
            if key == Key::None {
                continue;
            }
            if let Some(slot) = unique_maps[index].get(&key) {
                conflict = Some(*slot);
                break;
            }
        }

        match conflict {
            None => {
                for (index, column_name) in &unique_columns {
                    let key = Key::try_from(&row[*index])?;
                    if key != Key::None && unique_maps[index].contains_key(&key) {
                        return Err(ValidateError::DuplicateEntryOnUniqueField(
                            row[*index].clone(),
                            column_name.clone(),
                        )
                        .into());
                    }
                }

                let slot = Slot::Fresh(fresh.len());
                for (index, _) in &unique_columns {
                    let key = Key::try_from(&row[*index])?;
                    if key != Key::None
                        && let Some(map) = unique_maps.get_mut(index)
                    {
                        map.insert(key, slot);
                    }
                }
                fresh.push(row);
                affected.push(slot);
                affected_seen.insert(slot);
                rows_affected += 1;
            }
            Some(slot) => {
                let Some(update) = &update else {
                    continue;
                };

                let base_values = match slot {
                    Slot::Existing(position) => existing[position].1.clone(),
                    Slot::Fresh(position) => fresh[position].clone(),
                };
                let base_row = Row {
                    columns: Rc::clone(&column_names),
                    values: base_values,
                };
                let excluded_row = Row {
                    columns: Rc::clone(&column_names),
                    values: row,
                };

                if let Some(expr) = selection {
                    let next = Rc::new(RowContext::new(
                        "excluded",
                        Cow::Borrowed(&excluded_row),
                        None,
                    ));
                    let context = Rc::new(RowContext::new(
                        table_name,
                        Cow::Borrowed(&base_row),
                        Some(next),
                    ));

                    if !check_expr(storage, Some(&context), None, expr)? {
                        continue;
                    }
                }

                let old_values = base_row.values.clone();
                let new_row =
                    update.apply_with(base_row, foreign_keys, Some(("excluded", &excluded_row)))?;
                let new_values = new_row.values;

                for (index, column_name) in &unique_columns {
                    let old_key = Key::try_from(&old_values[*index])?;
                    let new_key = Key::try_from(&new_values[*index])?;
                    if old_key == new_key {
                        continue;
                    }
                    if new_key != Key::None && unique_maps[index].contains_key(&new_key) {
                        return Err(ValidateError::DuplicateEntryOnUniqueField(
                            new_values[*index].clone(),
                            column_name.clone(),
                        )
                        .into());
                    }
                    if let Some(map) = unique_maps.get_mut(index) {
                        map.remove(&old_key);
                        if new_key != Key::None {
                            map.insert(new_key, slot);
                        }
                    }
                }

                match slot {
                    Slot::Existing(position) => {
                        existing[position].1 = new_values;
                        dirty.insert(position);
                    }
                    Slot::Fresh(position) => fresh[position] = new_values,
                }
                if affected_seen.insert(slot) {
                    affected.push(slot);
                }
                rows_affected += 1;
            }
        }
    }

    schemaful::validate_foreign_key(storage, column_defs, foreign_keys.to_vec(), &fresh)?;

    let affected_rows = affected
        .iter()
        .map(|slot| match slot {
            Slot::Existing(position) => existing[*position].1.clone(),
            Slot::Fresh(position) => fresh[*position].clone(),
        })
        .collect::<Vec<_>>();

    let mut keyed: Vec<(Key, Vec<Value>)> = existing
        .into_iter()
        .enumerate()
        .filter(|(position, _)| dirty.contains(position))
        .map(|(_, entry)| entry)
        .collect();

    let write = if let Some(index) = schemaful::primary_key_index(column_defs) {
        for values in fresh {
            let key = Key::try_from(&values[index])?;
            keyed.push((key, values));
        }

        PendingWrite {
            keyed: Some(keyed),
            append: Vec::new(),
        }
    } else {
        PendingWrite {
            keyed: (!keyed.is_empty()).then_some(keyed),
            append: fresh,
        }
    };

    Ok(UpsertOutcome {
        rows_affected,
        affected_rows,
        write,
    })
}
