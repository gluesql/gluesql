use {
    super::FilterContext,
    crate::{convert_where_query, Condition, Link, Result, RowIter, Store},
    fstrings::*,
    itertools::{EitherOrBoth, Itertools},
    sled::IVec,
    sqlparser::ast::Expr,
};

pub type KeyIter = Box<dyn Iterator<Item = IVec>>;

pub fn check_expr<T: 'static>(
    storage: &dyn Store<T>,
    context: FilterContext,
    expr: &Expr,
) -> Result<bool> {
    match convert_where_query(expr) {
        Ok(Link::Condition(Condition::True)) => Ok(true),
        Ok(Link::Condition(Condition::False)) => Ok(false),
        Ok(other) => scan(context, other, storage),
        Err(error) => Err(error),
    }
}

fn scan<T: 'static>(context: FilterContext, link: Link, storage: &dyn Store<T>) -> KeyIter {
    // Todo: only do this if needed
    let all_rows = storage.scan_data(table);
    let all_keys = all_rows.map(|(key, _)| key);
    eval_link(link, storage.clone, table, &all_keys).map(|key| storage.get(f!("{prefix}/{key}")))
}

fn eval_link<T: 'static>(
    link: Link,
    storage: &dyn Store<T>,
    table: &str,
    all_keys: &KeyIter,
    all_rows: &RowIter<T>,
) -> KeyIter {
    match link {
        Link::And(link_a, link_b) => eval_link(link_a, storage.clone(), table, all_keys)
            .merge_join_by(eval_link(link_b, storage, table, all_keys))
            .filter_map(|item| {
                if let EitherOrBoth::Both(key) = item {
                    Some(key)
                } else {
                    None
                }
            }),
        Link::Or(link_a, link_b) => eval_link(link_a, storage.clone, table, all_keys)
            .merge_join_by(eval_link(link_b, storage, table, all_keys))
            .map(|item| item.unwrap()),
        Link::Not(link) => all_keys
            .merge_join_by(eval_link(link, storage.clone(), table, all_keys))
            .filter_map(|item| {
                if let EitherOrBoth::Left(key) = item {
                    Some(key)
                } else {
                    None
                }
            }),
        Link::Condition(condition) => eval_condition(condition, storage, table, all_keys),
    }
}

fn eval_condition<T: 'static>(
    condition: Condition,
    storage: &dyn Store<T>,
    table: &str,
    all_keys: &KeyIter,
    all_rows: &RowIter<T>,
) -> KeyIter {
    if condition.column.is_indexed {
        get_indexed(condition, storage, table)
    } else {
        let all_rows_keyed: TupleIterator = (all_keys, all_rows);
        all_rows_keyed.filter_map(|key, row| {
            let value = row.0.get_value(condition.column.index);
            if match condition {
                Condition::True => true,
                Condition::False => false,
                Condition::Equals { column_name, value } => value == condition.value,
                Condition::GreaterThanOrEquals { column_name, value } => value >= condition.value,
                Condition::LessThanOrEquals { column_name, value } => value <= condition.value,
                Condition::GreaterThan { column_name, value } => value > condition.value,
                Condition::LessThan { column_name, value } => value < condition.value,
                _ => false,
            } {
                Some(key)
            } else {
                None
            }
        });
    }
}
