use {
    crate::data::{Row, Value},
    std::{fmt::Debug, rc::Rc},
};

#[derive(Debug)]
pub enum RowContext<'a> {
    Data {
        table_alias: &'a str,
        row: Content<'a>,
        next: Option<Rc<RowContext<'a>>>,
    },
    Bridge {
        left: Rc<RowContext<'a>>,
        right: Rc<RowContext<'a>>,
    },
}

impl<'a> RowContext<'a> {
    pub fn new<T: Into<Content<'a>>>(
        table_alias: &'a str,
        row: T,
        next: Option<Rc<RowContext<'a>>>,
    ) -> Self {
        Self::Data {
            table_alias,
            row: row.into(),
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
            } if *table_alias == alias => Some(row.values()),
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
            } => row.values(),
            Self::Data {
                row,
                next: Some(next),
                ..
            } => [next.get_all_values(), row.values()].concat(),
            Self::Bridge { left, right } => {
                [left.get_all_values(), right.get_all_values()].concat()
            }
        }
    }
}

#[derive(Debug)]
pub enum Content<'a> {
    Borrowed(&'a Row),
    Owned(Row),
    Shared(Rc<Row>),
}

impl AsRef<Row> for Content<'_> {
    fn as_ref(&self) -> &Row {
        match self {
            Content::Borrowed(row) => row,
            Content::Owned(row) => row,
            Content::Shared(row) => row,
        }
    }
}

impl<'a> Content<'a> {
    fn get_value(&'a self, target: &str) -> Option<&'a Value> {
        self.as_ref().get_value(target)
    }

    fn values(&self) -> Vec<Value> {
        self.as_ref().values.clone()
    }
}

impl<'a> From<&'a Row> for Content<'a> {
    fn from(row: &'a Row) -> Self {
        Self::Borrowed(row)
    }
}

impl From<Row> for Content<'_> {
    fn from(row: Row) -> Self {
        Self::Owned(row)
    }
}

impl From<Rc<Row>> for Content<'_> {
    fn from(row: Rc<Row>) -> Self {
        Self::Shared(row)
    }
}
