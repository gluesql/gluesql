use std::rc::Rc;

#[derive(Debug)]
pub enum Context<'a> {
    Data {
        alias: String,
        columns: Vec<&'a str>,
        /// Optional vector containing the names of the primary key columns.
        /// If the vector is empty, it means that the table associated to the context does not have a primary key.
        primary_key: Option<Vec<&'a str>>,
        next: Option<Rc<Context<'a>>>,
    },
    Bridge {
        left: Rc<Context<'a>>,
        right: Rc<Context<'a>>,
    },
}

impl<'a> Context<'a> {
    pub fn new(
        alias: String,
        columns: Vec<&'a str>,
        primary_key: Option<Vec<&'a str>>,
        next: Option<Rc<Context<'a>>>,
    ) -> Self {
        Context::Data {
            alias,
            columns,
            primary_key,
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
                .map(|next| next.contains_alias(target))
                .unwrap_or(false),
            Self::Bridge { left, right } => {
                left.contains_alias(target) || right.contains_alias(target)
            }
        }
    }

    pub fn contains_column(&self, target: &str) -> bool {
        match self {
            Self::Data { columns, .. } if columns.iter().any(|column| column == &target) => true,
            Self::Data { next, .. } => next
                .as_ref()
                .map(|next| next.contains_column(target))
                .unwrap_or(false),
            Self::Bridge { left, right } => {
                left.contains_column(target) || right.contains_column(target)
            }
        }
    }

    pub fn contains_aliased_column(&self, target_alias: &str, target_column: &str) -> bool {
        match self {
            Self::Data { alias, columns, .. } if alias == target_alias => {
                columns.iter().any(|column| column == &target_column)
            }
            Self::Data { next, .. } => next
                .as_ref()
                .map(|next| next.contains_aliased_column(target_alias, target_column))
                .unwrap_or(false),
            Self::Bridge { left, right } => {
                left.contains_aliased_column(target_alias, target_column)
                    || right.contains_aliased_column(target_alias, target_column)
            }
        }
    }

    /// Returns the number of columns composing the primary key of the current context.
    pub(super) fn number_of_primary_key_columns(&self) -> usize {
        match self {
            Self::Data {
                primary_key, next, ..
            } => {
                if let Some(primary_key) = primary_key {
                    primary_key.len()
                } else {
                    next.as_ref()
                        .map_or(0, |next| next.number_of_primary_key_columns())
                }
            }
            Self::Bridge { left, right } => {
                left.number_of_primary_key_columns() + right.number_of_primary_key_columns()
            }
        }
    }

    /// Returns the index curresponding to the primary key column in the given candidate column.
    ///
    /// # Arguments
    /// * `candidate_column_name` - The name of the candidate column.
    ///
    /// # Returns
    /// The index of the primary key column in the candidate column, if the candidate column is a primary key column,
    /// otherwise `None`.
    ///
    pub(super) fn get_primary_key_index_by_name(
        &self,
        candidate_column_name: &str,
    ) -> Option<usize> {
        match self {
            Self::Data {
                primary_key, next, ..
            } => primary_key
                .as_ref()
                .and_then(|primary_key| {
                    primary_key
                        .iter()
                        .position(|column| column == &candidate_column_name)
                })
                .or_else(|| {
                    next.as_ref()
                        .and_then(|next| next.get_primary_key_index_by_name(candidate_column_name))
                }),
            Self::Bridge { left, right } => left
                .get_primary_key_index_by_name(candidate_column_name)
                .or_else(|| right.get_primary_key_index_by_name(candidate_column_name)),
        }
    }
}
