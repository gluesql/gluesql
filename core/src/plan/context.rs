use std::rc::Rc;

enum Lookup<T> {
    Unbound,
    Found(T),
    Ambiguous,
}

impl<T> Lookup<T> {
    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Ambiguous, _) | (_, Self::Ambiguous) | (Self::Found(_), Self::Found(_)) => {
                Self::Ambiguous
            }
            (found @ Self::Found(_), Self::Unbound) | (Self::Unbound, found @ Self::Found(_)) => {
                found
            }
            (Self::Unbound, Self::Unbound) => Self::Unbound,
        }
    }
}

enum AliasLookup {
    Unbound,
    Bound(bool),
}

pub enum Context<'a> {
    Data {
        alias: String,
        columns: Vec<&'a str>,
        next: Option<Rc<Context<'a>>>,
    },
    Bridge {
        left: Rc<Context<'a>>,
        right: Rc<Context<'a>>,
    },
}

impl<'a> Context<'a> {
    pub fn new(alias: String, columns: Vec<&'a str>, next: Option<Rc<Context<'a>>>) -> Self {
        Context::Data {
            alias,
            columns,
            next,
        }
    }

    pub fn concat(
        left: Option<Rc<Context<'a>>>,
        right: Option<Rc<Context<'a>>>,
    ) -> Option<Rc<Self>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(Rc::new(Self::Bridge { left, right })),
            (context @ Some(_), None) | (None, context @ Some(_)) => context,
            (None, None) => None,
        }
    }

    pub fn contains_alias(&self, target: &str) -> bool {
        match self {
            Self::Data { alias, .. } if alias == target => true,
            Self::Data { next, .. } => next
                .as_ref()
                .is_some_and(|next| next.contains_alias(target)),
            Self::Bridge { left, right } => {
                left.contains_alias(target) || right.contains_alias(target)
            }
        }
    }

    pub fn contains_column(&self, target: &str) -> bool {
        matches!(self.lookup_column(target), Lookup::Found(()))
    }

    fn lookup_column(&self, target: &str) -> Lookup<()> {
        match self {
            Self::Data { columns, next, .. } => {
                let current = if columns.iter().any(|column| column == &target) {
                    Lookup::Found(())
                } else {
                    Lookup::Unbound
                };

                let Some(next) = next else {
                    return current;
                };

                match next.as_ref() {
                    Self::Bridge { left, right } => {
                        let current = current.combine(left.lookup_column(target));
                        match current {
                            Lookup::Unbound => right.lookup_column(target),
                            found => found,
                        }
                    }
                    next @ Self::Data { .. } => current.combine(next.lookup_column(target)),
                }
            }
            Self::Bridge { left, right } => match left.lookup_column(target) {
                Lookup::Unbound => right.lookup_column(target),
                found => found,
            },
        }
    }

    pub fn contains_aliased_column(&self, target_alias: &str, target_column: &str) -> bool {
        matches!(
            self.lookup_aliased_column(target_alias, target_column),
            AliasLookup::Bound(true)
        )
    }

    fn lookup_aliased_column(&self, target_alias: &str, target_column: &str) -> AliasLookup {
        match self {
            Self::Data { alias, columns, .. } if alias == target_alias => {
                AliasLookup::Bound(columns.iter().any(|column| column == &target_column))
            }
            Self::Data { next, .. } => next.as_ref().map_or(AliasLookup::Unbound, |next| {
                next.lookup_aliased_column(target_alias, target_column)
            }),
            Self::Bridge { left, right } => {
                match left.lookup_aliased_column(target_alias, target_column) {
                    AliasLookup::Unbound => {
                        right.lookup_aliased_column(target_alias, target_column)
                    }
                    bound @ AliasLookup::Bound(_) => bound,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::Context, std::rc::Rc};

    #[test]
    fn aliased_column_does_not_fall_back_past_bound_alias() {
        let outer = Rc::new(Context::new("A".to_owned(), vec!["name"], None));
        let inner = Rc::new(Context::new("A".to_owned(), vec!["id"], None));
        let scope = Context::concat(Some(inner), Some(outer)).unwrap();

        assert!(!scope.contains_aliased_column("A", "name"));
    }

    #[test]
    fn unqualified_column_is_not_ambiguous_across_scopes() {
        let outer = Rc::new(Context::new("A".to_owned(), vec!["id"], None));
        let inner = Rc::new(Context::new("B".to_owned(), vec!["id"], None));
        let scope = Context::concat(Some(inner), Some(outer)).unwrap();

        assert!(scope.contains_column("id"));
    }

    #[test]
    fn unqualified_column_is_ambiguous_within_scope() {
        let left = Rc::new(Context::new("A".to_owned(), vec!["id"], None));
        let joined = Context::new("B".to_owned(), vec!["id"], Some(left));

        assert!(!joined.contains_column("id"));
    }
}
