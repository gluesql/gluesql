use std::rc::Rc;

pub enum Context<'a> {
    Data {
        alias: String,
        columns: Vec<&'a str>,
        primary_key: Option<&'a str>,
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
        primary_key: Option<&'a str>,
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

    /// Returns a mask of booleans representing which primary key columns are present in the current expression.
    ///
    /// # Arguments
    /// * `primary_key_columns` - The primary key columns to check for.
    ///
    /// # Implementative details
    /// The function is implemented as a recursive function that traverses the expression tree
    /// and returns a mask of booleans representing which primary key columns are present in the current expression.
    fn primary_key_mask(&self, primary_key_columns: &[&str]) -> Vec<bool> {
        match self {
            Self::Data {
                primary_key, next, ..
            } => {
                let mut mask = next
                    .as_ref()
                    .map(|next: &Rc<Context<'a>>| next.primary_key_mask(primary_key_columns))
                    .unwrap_or(vec![false; primary_key_columns.len()]);

                if let Some(primary_key) = primary_key {
                    primary_key_columns
                        .iter()
                        .zip(mask.iter_mut())
                        .for_each(|(column, mask)| {
                            if column == primary_key {
                                *mask = true;
                            }
                        });
                }

                mask
            }
            Self::Bridge { left, right } => left
                .primary_key_mask(primary_key_columns)
                .into_iter()
                .zip(right.primary_key_mask(primary_key_columns))
                .map(|(left, right)| left || right)
                .collect(),
        }
    }

    /// Returns whether the current expression contains all of the given primary key columns.
    ///
    /// # Arguments
    /// * `primary_key_columns` - The primary key columns to check for.
    ///
    /// # Implementative details
    /// A primary key is considered to be present in the expression if all of its columns can
    /// be found in the expression tree.
    pub fn contains_primary_key(&self, primary_key_columns: &[&str]) -> bool {
        self.primary_key_mask(primary_key_columns)
            .iter()
            .all(|mask| *mask)
    }
}
