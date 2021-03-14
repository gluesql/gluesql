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
        &all_keys,
        &all_rows,
    )
    .await;
    let scan_rows = Box::new(
        stream::iter(scan_keys)
            .filter_map(|key| async {
                Some((key, storage.get_by_key(table_name, key).await.ok()?))
            }) // Probably shouldn'Key be doing this...
            .collect(),
    );

    Ok(scan_rows)
}

async fn eval_link<Key: 'static + Debug + Eq + Ord, Storage: Store<Key> + Index<Key>>(
    link: &Link,
    storage: &Storage,
    table_name: &str,
    columns: Rc<[Ident]>,
    all_keys: &KeyIter<Key>,
    all_rows: &RowIter<Key>,
) -> Result<Box<KeyIter<Key>>> {
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
            eval_condition(condition, storage, table_name, columns, all_keys, all_rows).await
        }
    })
}

async fn eval_condition<
    Key: 'static + Debug + Eq + Ord,
    Storage: Store<Key> + Index<Key>,
    KeyIter: Iterator<Item = Key> + Clone,
>(
    condition: &Condition,
    storage: &Storage,
    table_name: &str,
    columns: Rc<[Ident]>,
    all_keys: &KeyIter,
    all_rows: &RowIter<Key>,
) -> KeyIter {
    if false {
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
