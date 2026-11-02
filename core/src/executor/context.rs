mod aggregate_context;
mod row_context;

pub use {
    aggregate_context::{AggregateContext, AggregateValues},
    row_context::RowContext,
};

use {
    crate::data::RegexCache,
    std::{cell::RefCell, rc::Rc},
};

thread_local! {
    static ACTIVE_REGEX_CACHE: RefCell<Option<Rc<RefCell<RegexCache>>>> = const { RefCell::new(None) };
}

pub(crate) struct ExecutionContext {
    cache: Rc<RefCell<RegexCache>>,
}

impl ExecutionContext {
    pub(crate) fn new() -> Rc<Self> {
        Rc::new(Self {
            cache: Rc::new(RefCell::new(RegexCache::new())),
        })
    }

    pub(crate) fn with_regex_cache<T>(operation: impl FnOnce(&mut RegexCache) -> T) -> T {
        ACTIVE_REGEX_CACHE.with(|active| {
            let cache = active
                .borrow()
                .as_ref()
                .cloned()
                .expect("regex cache used outside an execution scope");
            operation(&mut cache.borrow_mut())
        })
    }

    pub(crate) fn is_active() -> bool {
        ACTIVE_REGEX_CACHE.with(|active| active.borrow().is_some())
    }

    pub(crate) fn with_scope<T>(operation: impl FnOnce() -> T) -> T {
        if Self::is_active() {
            operation()
        } else {
            let context = Self::new();
            let _scope = context.activate();
            operation()
        }
    }

    /// Activates this context until the returned scope is dropped.
    ///
    /// Nested scopes must be dropped in reverse order of creation.
    pub(crate) fn activate(&self) -> ExecutionScope {
        let previous =
            ACTIVE_REGEX_CACHE.with(|active| active.borrow_mut().replace(Rc::clone(&self.cache)));

        ExecutionScope { previous }
    }
}

pub(crate) struct ExecutionScope {
    previous: Option<Rc<RefCell<RegexCache>>>,
}

impl Drop for ExecutionScope {
    fn drop(&mut self) {
        ACTIVE_REGEX_CACHE.with(|active| {
            *active.borrow_mut() = self.previous.take();
        });
    }
}

#[cfg(test)]
mod tests {
    use {super::ExecutionContext, crate::data::regex_with_cache};

    #[test]
    fn nested_execution_context_restores_previous_cache() {
        let outer = ExecutionContext::new();
        let outer_scope = outer.activate();
        ExecutionContext::with_regex_cache(|cache| {
            regex_with_cache("Hello", "Hello", true, cache).unwrap();
            assert_eq!(cache.len(), 1);
        });

        {
            let inner = ExecutionContext::new();
            let _inner_scope = inner.activate();
            ExecutionContext::with_regex_cache(|cache| assert_eq!(cache.len(), 0));
        }

        ExecutionContext::with_regex_cache(|cache| assert_eq!(cache.len(), 1));
        drop(outer_scope);
        drop(outer);
    }
}
