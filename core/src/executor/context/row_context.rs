use {
    crate::data::{Row, Value},
    std::{borrow::Cow, collections::BTreeMap, fmt::Debug, sync::Arc},
};

#[derive(Debug)]
pub enum RowContext<'a> {
    Data {
        table_alias: &'a str,
        row: Cow<'a, Row>,
        next: Option<Arc<RowContext<'a>>>,
    },
    RefVecData {
        columns: &'a [String],
        values: &'a [Value],
    },
    RefMapData(&'a BTreeMap<String, Value>),
    Bridge {
        left: Arc<RowContext<'a>>,
        right: Arc<RowContext<'a>>,
    },
}

impl<'a> RowContext<'a> {
    pub fn new(table_alias: &'a str, row: Cow<'a, Row>, next: Option<Arc<RowContext<'a>>>) -> Self {
        Self::Data {
            table_alias,
            row,
            next,
        }
    }

    pub fn concat(left: Arc<RowContext<'a>>, right: Arc<RowContext<'a>>) -> Self {
        Self::Bridge { left, right }
    }

    pub fn get_value(&'a self, target: &str) -> Option<&'a Value> {
        match self {
            Self::Data {
                row, next: None, ..
            } => row.get_value(target),
            Self::Data {
                row,
                next: Some(next),
                ..
            } => row.get_value(target).or_else(|| next.get_value(target)),
            Self::Bridge { left, right } => {
                left.get_value(target).or_else(|| right.get_value(target))
            }
            Self::RefVecData { columns, values } => columns
                .iter()
                .position(|column| column == target)
                .and_then(|index| values.get(index)),
            Self::RefMapData(values) => values.get(target),
        }
    }

    pub fn get_alias_value(&'a self, target_table_alias: &str, target: &str) -> Option<&'a Value> {
        match self {
            Self::Data {
                table_alias,
                row,
                next,
            } if *table_alias == target_table_alias => {
                let value = row.get_value(target);

                if value.is_some() {
                    value
                } else {
                    next.as_ref()
                        .and_then(|context| context.get_alias_value(target_table_alias, target))
                }
            }
            Self::Data {
                next: Some(next), ..
            } => next.get_alias_value(target_table_alias, target),
            Self::Bridge { left, right } => left
                .get_alias_value(target_table_alias, target)
                .or_else(|| right.get_alias_value(target_table_alias, target)),
            _ => None,
        }
    }

    pub fn get_alias_entries(&self, alias: &str) -> Option<Vec<(&String, Value)>> {
        match self {
            Self::Data {
                table_alias, row, ..
            } if *table_alias == alias => Some(row.iter().map(|(k, v)| (k, v.clone())).collect()),
            Self::Data {
                next: Some(next), ..
            } => next.get_alias_entries(alias),
            Self::Bridge { left, right } => left
                .get_alias_entries(alias)
                .or_else(|| right.get_alias_entries(alias)),
            _ => None,
        }
    }

    pub fn get_all_entries(&self) -> Vec<(&String, Value)> {
        match self {
            Self::Data {
                row, next: None, ..
            } => row.iter().map(|(k, v)| (k, v.clone())).collect(),
            Self::Data {
                row,
                next: Some(next),
                ..
            } => next
                .get_all_entries()
                .into_iter()
                .chain(row.iter().map(|(k, v)| (k, v.clone())))
                .collect(),
            Self::Bridge { left, right } => {
                [left.get_all_entries(), right.get_all_entries()].concat()
            }
            _ => vec![],
        }
    }
}
