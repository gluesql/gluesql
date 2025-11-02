use std::sync::Arc;

pub enum Context<'a> {
    Data {
        alias: String,
        columns: Vec<&'a str>,
        primary_key: Option<&'a str>,
        next: Option<Arc<Context<'a>>>,
    },
    Bridge {
        left: Arc<Context<'a>>,
        right: Arc<Context<'a>>,
    },
}

impl<'a> Context<'a> {
    pub fn new(
        alias: String,
        columns: Vec<&'a str>,
        primary_key: Option<&'a str>,
        next: Option<Arc<Context<'a>>>,
    ) -> Self {
        Context::Data {
            alias,
            columns,
            primary_key,
            next,
        }
    }

    pub fn concat(
        left: Option<Arc<Context<'a>>>,
        right: Option<Arc<Context<'a>>>,
    ) -> Option<Arc<Self>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(Arc::new(Self::Bridge { left, right })),
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
        match self {
            Self::Data { columns, .. } if columns.iter().any(|column| column == &target) => true,
            Self::Data { next, .. } => next
                .as_ref()
                .is_some_and(|next| next.contains_column(target)),
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
                .is_some_and(|next| next.contains_aliased_column(target_alias, target_column)),
            Self::Bridge { left, right } => {
                left.contains_aliased_column(target_alias, target_column)
                    || right.contains_aliased_column(target_alias, target_column)
            }
        }
    }

    pub fn contains_primary_key(&self, target_column: &str) -> bool {
        match self {
            Self::Data {
                primary_key: Some(primary_key),
                ..
            } if primary_key == &target_column => true,
            Self::Data { next, .. } => next
                .as_ref()
                .is_some_and(|next| next.contains_primary_key(target_column)),
            Self::Bridge { left, right } => {
                left.contains_primary_key(target_column)
                    || right.contains_primary_key(target_column)
            }
        }
    }
}
