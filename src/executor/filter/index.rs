use {
    super::Filter,
    crate::{
        convert_where_query, data::Row, Condition, Error, Index, Link, Result, RowIter, Store,
    },
    fstrings::*,
    futures::stream::{self, StreamExt, TryStream, TryStreamExt},
    itertools::{EitherOrBoth, Itertools},
    sled::IVec,
    sqlparser::ast::Ident,
    std::fmt::Debug,
    std::rc::Rc,
};

pub type KeyIter<Key> = Box<dyn Iterator<Item = Key>>;

pub async fn fetch<'a, Key: 'static + Debug + Eq + Ord, Storage: Store<Key> + Index<Key>>(
    storage: &'a Storage,
    table_name: &str,
    columns: Rc<[Ident]>,
    filter: Filter<'a, Key>,
) -> Result<impl TryStream<Ok = (Rc<[Ident]>, Key, Row), Error = Error> + 'a> {
    let where_query = filter.where_clause;
    let where_query = where_query.map(|where_query| convert_where_query(where_query));
    match where_query {
        Some(Ok(Link::Condition(Condition::True))) | None => {
            scan(
                storage,
                table_name,
                columns,
                Link::Condition(Condition::True),
            )
            .await
        } //storage.scan_data(table_name).await,
        //Some(Ok(Link::Condition(Condition::False))) => Ok(false),
        Some(Ok(other)) => scan(storage, table_name, columns, other).await,
        Some(Err(error)) => Err(error),
    }
}

async fn scan<Key: 'static + Debug + Eq + Ord, Storage: Store<Key> + Index<Key>>(
    storage: &Storage,
    table_name: &str,
    columns: Rc<[Ident]>,
    link: Link,
) -> Result<impl TryStream<Ok = (Rc<[Ident]>, Key, Row), Error = Error>> {
    let all_rows: RowIter<Key> = storage.scan_data(table_name).await?;
    let all_keys: KeyIter<Key> = Box::new(
        all_rows
            .filter_map(|result| result.ok().map(|(key, _)| key))
            .collect::<Vec<Key>>() // this seems unnecessary
            .into_iter(),
    );

    let scan_keys = eval_link(
        &link,
        storage.clone(),
        table_name,
        columns,
        all_keys,
        all_rows,
    )
    .await?;
    let scan_rows: Vec<(Rc<[Ident]>, Key, Row)> = stream::iter(scan_keys)
        .filter_map(|key| async {
            Some((
                columns,
                key,
                storage.get_by_key(table_name, key).await.ok()?,
            ))
        })
        .collect::<Vec<(Rc<[Ident]>, Key, Row)>>()
        .await;

    let test = Ok(stream::iter(scan_rows));
    test
}

async fn eval_link<Key: 'static + Debug + Eq + Ord, Storage: Store<Key> + Index<Key>>(
    link: &Link,
    storage: &Storage,
    table_name: &str,
    columns: Rc<[Ident]>,
    all_keys: KeyIter<Key>,
    all_rows: RowIter<Key>,
) -> Result<KeyIter<Key>> {
    macro_rules! eval_link {
        ($link:expr) => {
            eval_link(
                $link,
                storage,
                table_name,
                columns.clone(),
                all_keys,
                all_rows,
            )
            .await?
        };
    }
    macro_rules! merge {
        ($l:expr, $r:expr, $f:expr) => {
            Box::new(
                $l.merge_join_by($r, |l, r| l.cmp(r))
                    .filter_map($f)
                    .collect::<Vec<Key>>()
                    .into_iter(),
            )
        };
    }

    Ok(match link {
        Link::And(link_l, link_r) => merge!(eval_link!(link_l), eval_link!(link_r), |item| item
            .both()
            .map(|(l, r)| l)),
        Link::Or(link_l, link_r) => merge!(eval_link!(link_l), eval_link!(link_r), |item| if item
            .is_right()
        {
            item.right()
        } else {
            item.left()
        }),
        Link::Not(link) => merge!(all_keys, eval_link!(link), |item| if item.is_left() {
            item.left()
        } else {
            None
        }),
        Link::Condition(condition) => {
            eval_condition(condition, storage, table_name, columns, all_keys, all_rows).await?
        }
    })
}

async fn eval_condition<'a, Key: 'static + Debug + Eq + Ord, Storage: Store<Key> + Index<Key>>(
    condition: &Condition,
    storage: &Storage,
    table_name: &str,
    columns: Rc<[Ident]>,
    all_keys: KeyIter<Key>,
    all_rows: RowIter<Key>,
) -> Result<KeyIter<Key>> {
    if false {
        Ok(Box::new(
            storage
                .get_indexed_keys(condition, table_name)
                .await?
                .into_iter(),
        ))
    } else {
        Ok(Box::new(
            all_rows
                .filter_map(|result| {
                    let (key, row) = result.ok()?;
                    let get_value = |column_name| {
                        row.get_value(get_column_index_by_name(columns, column_name)?)
                    };
                    macro_rules! compare {
                ($value:ident, $op:tt, $column_name:ident) => {
                    Some($value $op get_value($column_name)?)
                }
            };
                    let check = match condition {
                        Condition::True => Some(true),
                        Condition::False => Some(false),
                        Condition::Equals { column_name, value } => {
                            compare!(value, ==, column_name)
                        }
                        Condition::GreaterThanOrEquals { column_name, value } => {
                            compare!(value, >=, column_name)
                        }
                        Condition::LessThanOrEquals { column_name, value } => {
                            compare!(value, <=, column_name)
                        }
                        Condition::GreaterThan { column_name, value } => {
                            compare!(value, >, column_name)
                        }
                        Condition::LessThan { column_name, value } => {
                            compare!(value, <, column_name)
                        }
                        _ => None,
                    };
                    if matches!(check, Some(true)) {
                        Some(key)
                    } else {
                        None
                    }
                })
                .collect::<Vec<Key>>()
                .into_iter(),
        ))
    }
}

fn get_column_index_by_name(columns: Rc<[Ident]>, name: &str) -> Option<usize> {
    columns.iter().position(|column| column.value == name)
}
