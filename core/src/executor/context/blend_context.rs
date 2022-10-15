use {
    crate::data::{Row, Value},
    std::{fmt::Debug, rc::Rc},
};

#[derive(Debug)]
pub enum BlendContextRow {
    Single(Option<Row>),
    Shared(Rc<Row>),
}

#[derive(Debug)]
pub struct BlendContext<'a> {
    table_alias: &'a str,
    columns: Rc<[String]>,
    row: BlendContextRow,
    next: Option<Rc<BlendContext<'a>>>,
}

impl<'a> BlendContext<'a> {
    pub fn new(
        table_alias: &'a str,
        columns: Rc<[String]>,
        row: BlendContextRow,
        next: Option<Rc<BlendContext<'a>>>,
    ) -> Self {
        Self {
            table_alias,
            columns,
            row,
            next,
        }
    }

    pub fn get_value(&'a self, target: &str) -> Option<&'a Value> {
        let value = match &self.row {
            BlendContextRow::Shared(row) => row.get_value(&self.columns, target),
            BlendContextRow::Single(Some(row)) => row.get_value(&self.columns, target),
            BlendContextRow::Single(None) => Some(&Value::Null),
        };

        if value.is_some() {
            return value;
        }

        self.next
            .as_ref()
            .and_then(|context| context.get_value(target))
    }

    pub fn get_alias_value(&'a self, table_alias: &str, target: &str) -> Option<&'a Value> {
        let value = (|| {
            if self.table_alias != table_alias {
                return None;
            }

            match &self.row {
                BlendContextRow::Shared(row) => row.get_value(&self.columns, target),
                BlendContextRow::Single(Some(row)) => row.get_value(&self.columns, target),
                BlendContextRow::Single(None) => Some(&Value::Null),
            }
        })();

        if value.is_some() {
            return value;
        }

        self.next
            .as_ref()
            .and_then(|context| context.get_alias_value(table_alias, target))
    }

    pub fn get_alias_values(&self, alias: &str) -> Option<Vec<Value>> {
        if self.table_alias == alias {
            let values = match &self.row {
                BlendContextRow::Shared(row) => row.0.clone(),
                BlendContextRow::Single(Some(row)) => row.0.clone(),
                BlendContextRow::Single(None) => self.columns.iter().map(|_| Value::Null).collect(),
            };

            Some(values)
        } else {
            self.next
                .as_ref()
                .and_then(|next| next.get_alias_values(alias))
        }
    }

    pub fn get_all_values(&'a self) -> Vec<Value> {
        let values: Vec<Value> = match &self.row {
            BlendContextRow::Shared(row) => row.0.clone(),
            BlendContextRow::Single(Some(row)) => row.0.clone(),
            BlendContextRow::Single(None) => self.columns.iter().map(|_| Value::Null).collect(),
        };

        match &self.next {
            Some(next) => next
                .get_all_values()
                .into_iter()
                .chain(values.into_iter())
                .collect(),
            None => values,
        }
    }
}
