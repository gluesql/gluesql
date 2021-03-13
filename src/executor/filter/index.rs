use {
    super::Filter,
    crate::{
        convert_where_query, data::Row, Condition, Error, Index, Link, Result, RowIter, Store,
    },
    fstrings::*,
    futures::stream::TryStream,
    itertools::{EitherOrBoth, Itertools},
    sled::IVec,
    sqlparser::ast::Ident,
    std::fmt::Debug,
    std::rc::Rc,
};

pub type KeyIter = Box<dyn Iterator<Item = IVec>>;

pub async fn fetch<'a, T: 'static + Debug, U: Store<T> + Index>(
    storage: &U,
    table_name: &str,
    columns: Rc<[Ident]>,
    filter: Filter<'a, T>,
) -> Result<impl TryStream<Ok = (Rc<[Ident]>, T, Row), Error = Error> + 'a> {
    let where_query = filter.where_clause;
    let where_query = where_query.map(|where_query| convert_where_query(where_query));
    match where_query {
        Some(Ok(Link::Condition(Condition::True))) | None => storage.scan_data(table_name),
        Some(Ok(Link::Condition(Condition::False))) => Ok(false),
        Some(Ok(other)) => scan(storage, table_name, columns, other),
        Some(Err(error)) => Err(error),
    }
}

fn scan<T: 'static + Debug, U: Store<T> + Index>(
    storage: &U,
    table_name: &str,
    columns: Rc<[Ident]>,
    link: Link,
) -> Result<impl TryStream<Ok = (Rc<[Ident]>, T, Row), Error = Error>> {
    // Todo: only do this if needed
    let all_rows = storage.scan_data(table_name);
    let all_keys = all_rows.map(|(key, _)| key);
    let scan_keys = eval_link(link, storage.clone, table_name, &all_keys)
        .map(|key| storage.get_by_key(table_name, key));
    let scan_rows = scan_keys.map(|key| storage.get(key));
}

fn eval_link<T: 'static + Debug, U: Store<T> + Index>(
    link: Link,
    storage: &U,
    table_name: &str,
    columns: Rc<[Ident]>,
    all_keys: &KeyIter,
    all_rows: &RowIter<T>,
) -> KeyIter {
    match link {
        Link::And(link_a, link_b) => {
            eval_link(link_a, storage.clone(), table_name, columns, all_keys)
                .merge_join_by(eval_link(link_b, storage, table_name, columns, all_keys))
                .filter_map(|item| {
                    if let EitherOrBoth::Both(key) = item {
                        Some(key)
                    } else {
                        None
                    }
                })
        }
        Link::Or(link_a, link_b) => eval_link(link_a, storage.clone, table_name, columns, all_keys)
            .merge_join_by(eval_link(link_b, storage, table_name, columns, all_keys))
            .map(|item| item.unwrap()),
        Link::Not(link) => all_keys
            .merge_join_by(eval_link(
                link,
                storage.clone(),
                table_name,
                columns,
                all_keys,
            ))
            .filter_map(|item| {
                if let EitherOrBoth::Left(key) = item {
                    Some(key)
                } else {
                    None
                }
            }),
        Link::Condition(condition) => {
            eval_condition(condition, storage, table_name, columns, all_keys)
        }
    }
}

fn eval_condition<T: 'static + Debug, U: Store<T> + Index>(
    condition: Condition,
    storage: &U,
    table_name: &str,
    columns: Rc<[Ident]>,
    all_keys: &KeyIter,
    all_rows: &RowIter<T>,
) -> KeyIter {
    if condition.column.is_indexed {
        storage.get_indexed(condition, storage, table_name)
    } else {
        all_rows.filter_map(|key, row| {
            if match condition {
                Condition::True => true,
                Condition::False => false,
                Condition::Equals { column_name, value } => {
                    value == row.get_value(get_column_index_by_name(columns, column_name))
                }
                Condition::GreaterThanOrEquals { column_name, value } => {
                    value >= row.get_value(get_column_index_by_name(columns, column_name))
                }
                Condition::LessThanOrEquals { column_name, value } => {
                    value <= row.get_value(get_column_index_by_name(columns, column_name))
                }
                Condition::GreaterThan { column_name, value } => {
                    value > row.get_value(get_column_index_by_name(columns, column_name))
                }
                Condition::LessThan { column_name, value } => {
                    value < row.get_value(get_column_index_by_name(columns, column_name))
                }
                _ => false,
            } {
                Some(key)
            } else {
                None
            }
        });
    }
}

fn get_column_index_by_name(columns: Rc<[Ident]>, name: &str) -> usize {
    columns.iter().position(|column| column.name == name)
}
