use {
    crate::data::{Row, SCHEMALESS_DOC_COLUMN, Value},
    std::{borrow::Cow, fmt::Debug, rc::Rc},
};

#[derive(Debug)]
pub enum RowContext<'a> {
    Data {
        table_alias: &'a str,
        row: Cow<'a, Row>,
        next: Option<Rc<RowContext<'a>>>,
    },
    RefVecData {
        columns: &'a [String],
        values: &'a [Value],
    },
    Bridge {
        left: Rc<RowContext<'a>>,
        right: Rc<RowContext<'a>>,
    },
}

#[derive(Debug, PartialEq)]
pub enum ValueLookup<'a> {
    Unbound,
    Missing,
    Found(&'a Value),
    Ambiguous,
}

impl ValueLookup<'_> {
    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Ambiguous, _) | (_, Self::Ambiguous) | (Self::Found(_), Self::Found(_)) => {
                Self::Ambiguous
            }
            (found @ Self::Found(_), Self::Unbound | Self::Missing)
            | (Self::Unbound | Self::Missing, found @ Self::Found(_)) => found,
            (Self::Missing, Self::Missing | Self::Unbound) | (Self::Unbound, Self::Missing) => {
                Self::Missing
            }
            (Self::Unbound, Self::Unbound) => Self::Unbound,
        }
    }

    fn or_else(self, other: impl FnOnce() -> Self) -> Self {
        match self {
            Self::Unbound => other(),
            Self::Missing => match other() {
                Self::Unbound => Self::Missing,
                found => found,
            },
            found => found,
        }
    }
}

enum AliasLookup<T> {
    Unbound,
    Bound(T),
}

fn lookup_row_value<'a>(row: &'a Row, target: &str) -> ValueLookup<'a> {
    if let Some(value) = row.get_value(target) {
        return ValueLookup::Found(value);
    }

    let Some(Value::Map(document)) = row.get_value(SCHEMALESS_DOC_COLUMN) else {
        return ValueLookup::Unbound;
    };

    document
        .get(target)
        .map_or(ValueLookup::Missing, ValueLookup::Found)
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
        match self.lookup_value(target) {
            ValueLookup::Found(value) => Some(value),
            ValueLookup::Unbound | ValueLookup::Missing | ValueLookup::Ambiguous => None,
        }
    }

    pub fn lookup_value(&'a self, target: &str) -> ValueLookup<'a> {
        match self {
            Self::Data { row, next, .. } => {
                let current = lookup_row_value(row, target);

                let Some(next) = next else {
                    return current;
                };

                match next.as_ref() {
                    Self::Bridge { left, right } => {
                        let current = current.combine(left.lookup_value(target));
                        current.or_else(|| right.lookup_value(target))
                    }
                    next => current.combine(next.lookup_value(target)),
                }
            }
            Self::Bridge { left, right } => left
                .lookup_value(target)
                .or_else(|| right.lookup_value(target)),
            Self::RefVecData { columns, values } => columns
                .iter()
                .position(|column| column == target)
                .and_then(|index| values.get(index))
                .map_or(ValueLookup::Unbound, ValueLookup::Found),
        }
    }

    pub fn get_alias_value(&'a self, target_table_alias: &str, target: &str) -> Option<&'a Value> {
        match self.lookup_alias_value(target_table_alias, target) {
            AliasLookup::Bound(value) => value,
            AliasLookup::Unbound => None,
        }
    }

    fn lookup_alias_value(
        &'a self,
        target_table_alias: &str,
        target: &str,
    ) -> AliasLookup<Option<&'a Value>> {
        match self {
            Self::Data {
                table_alias, row, ..
            } if *table_alias == target_table_alias => {
                let value = match lookup_row_value(row, target) {
                    ValueLookup::Found(value) => Some(value),
                    ValueLookup::Unbound | ValueLookup::Missing | ValueLookup::Ambiguous => None,
                };
                AliasLookup::Bound(value)
            }
            Self::Data {
                next: Some(next), ..
            } => next.lookup_alias_value(target_table_alias, target),
            Self::Bridge { left, right } => {
                match left.lookup_alias_value(target_table_alias, target) {
                    AliasLookup::Unbound => right.lookup_alias_value(target_table_alias, target),
                    bound @ AliasLookup::Bound(_) => bound,
                }
            }
            _ => AliasLookup::Unbound,
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

#[cfg(test)]
mod tests {
    use {
        super::{RowContext, ValueLookup},
        crate::data::{Row, SCHEMALESS_DOC_COLUMN, Value},
        std::{borrow::Cow, collections::BTreeMap, rc::Rc},
    };

    fn context(
        alias: &'static str,
        columns: &[&str],
        next: Option<Rc<RowContext<'static>>>,
    ) -> Rc<RowContext<'static>> {
        let row = Row {
            columns: columns
                .iter()
                .map(|column| (*column).to_owned())
                .collect::<Vec<_>>()
                .into(),
            values: (0..columns.len())
                .map(|index| Value::I64(index as i64))
                .collect(),
        };

        Rc::new(RowContext::new(alias, Cow::Owned(row), next))
    }

    fn document_context(
        alias: &'static str,
        entries: &[(&str, Value)],
        next: Option<Rc<RowContext<'static>>>,
    ) -> Rc<RowContext<'static>> {
        let document = entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let row = Row {
            columns: vec![SCHEMALESS_DOC_COLUMN.to_owned()].into(),
            values: vec![Value::Map(document)],
        };

        Rc::new(RowContext::new(alias, Cow::Owned(row), next))
    }

    #[test]
    fn qualified_alias_shadows_outer_scope_even_when_column_is_missing() {
        let outer = context("A", &["name"], None);
        let inner = context("A", &["id"], None);
        let scope = RowContext::concat(inner, outer);

        assert_eq!(scope.get_alias_value("A", "name"), None);
    }

    #[test]
    fn qualified_alias_falls_back_only_when_unbound() {
        let outer = context("A", &["name"], None);
        let inner = context("B", &["id"], None);
        let scope = RowContext::concat(inner, outer);

        assert_eq!(scope.get_alias_value("A", "name"), Some(&Value::I64(0)));
    }

    #[test]
    fn unqualified_lookup_detects_same_scope_ambiguity() {
        let left = context("A", &["id"], None);
        let joined = context("B", &["id"], Some(left));

        assert!(matches!(joined.lookup_value("id"), ValueLookup::Ambiguous));
    }

    #[test]
    fn unqualified_lookup_prefers_current_scope() {
        let outer = context("A", &["id"], None);
        let inner = context("B", &["id"], None);
        let scope = RowContext::concat(inner, outer);

        assert!(matches!(
            scope.lookup_value("id"),
            ValueLookup::Found(Value::I64(0))
        ));
    }

    #[test]
    fn unqualified_lookup_handles_unbound_and_bridge_chains() {
        let outer_left = context("A", &["name"], None);
        let outer_right = context("B", &["id"], None);
        let outer = Rc::new(RowContext::concat(outer_left, outer_right));
        let current = context("C", &["quantity"], Some(outer));

        assert!(matches!(
            current.lookup_value("id"),
            ValueLookup::Found(Value::I64(0))
        ));
        assert_eq!(current.get_value("missing"), None);
        assert_eq!(current.get_alias_value("missing", "id"), None);
    }

    #[test]
    fn missing_document_key_falls_back_only_to_a_bound_value() {
        let missing = document_context("A", &[], None);
        let unbound = context("B", &["name"], None);
        let scope = RowContext::concat(missing, unbound);
        assert!(matches!(scope.lookup_value("id"), ValueLookup::Missing));

        let missing = document_context("A", &[], None);
        let found = context("B", &["id"], None);
        let scope = RowContext::concat(missing, found);
        assert!(matches!(
            scope.lookup_value("id"),
            ValueLookup::Found(Value::I64(0))
        ));
    }

    #[test]
    fn schemaless_lookup_counts_actual_document_keys() {
        let left = document_context("A", &[("id", Value::I64(1))], None);
        let joined = document_context("B", &[("name", Value::Str("B".to_owned()))], Some(left));
        assert!(matches!(
            joined.lookup_value("id"),
            ValueLookup::Found(Value::I64(1))
        ));

        let left = document_context("A", &[("id", Value::I64(1))], None);
        let joined = document_context("B", &[("id", Value::I64(2))], Some(left));
        assert!(matches!(joined.lookup_value("id"), ValueLookup::Ambiguous));
    }
}
