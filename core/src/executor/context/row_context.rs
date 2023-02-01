use {
    crate::data::{Row, Value},
    std::{borrow::Cow, fmt::Debug, rc::Rc},
};

#[derive(Debug)]
pub enum RowContext<'a> {
    Data {
        table_alias: &'a str,
        row: Cow<'a, Row>,
        next: Option<Rc<RowContext<'a>>>,
    },
    Bridge {
        left: Rc<RowContext<'a>>,
        right: Rc<RowContext<'a>>,
    },
}

impl<'a> RowContext<'a> {
    pub fn new(table_alias: &'a str, row: Cow<'a, Row>, next: Option<Rc<RowContext<'a>>>) -> Self {
        Self::Data {
            table_alias,
            row,
            next,
        }
    }

    pub fn concat(left: Rc<RowContext<'a>>, right: Rc<RowContext<'a>>) -> Self {
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
            Self::Data { next: None, .. } => None,
            Self::Data {
                next: Some(next), ..
            } => next.get_alias_value(target_table_alias, target),
            Self::Bridge { left, right } => left
                .get_alias_value(target_table_alias, target)
                .or_else(|| right.get_alias_value(target_table_alias, target)),
        }
    }

    pub fn get_alias_values(&self, alias: &str) -> Option<Vec<Value>> {
        match self {
            Self::Data {
                table_alias, row, ..
            } if *table_alias == alias => Some(row.as_ref().values.clone()),
            Self::Data { next: None, .. } => None,
            Self::Data {
                next: Some(next), ..
            } => next.get_alias_values(alias),
            Self::Bridge { left, right } => left
                .get_alias_values(alias)
                .or_else(|| right.get_alias_values(alias)),
        }
    }

    pub fn get_all_values(&self) -> Vec<Value> {
        match self {
            Self::Data {
                row, next: None, ..
            } => row.as_ref().values.clone(),
            Self::Data {
                row,
                next: Some(next),
                ..
            } => [next.get_all_values(), row.as_ref().values.clone()].concat(),
            Self::Bridge { left, right } => {
                [left.get_all_values(), right.get_all_values()].concat()
            }
        }
    }
}
