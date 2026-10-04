use {
    std::sync::OnceLock,
    tracing::Span,
    tracing_subscriber::{EnvFilter, fmt::format::FmtSpan},
};

static DEFAULT_SUBSCRIBER: OnceLock<()> = OnceLock::new();

pub fn ensure_default_subscriber() {
    DEFAULT_SUBSCRIBER.get_or_init(|| {
        if tracing::dispatcher::has_been_set() {
            return;
        }

        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("gluesql=info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

pub struct TracedResultIterator<I, F> {
    inner: I,
    span: Span,
    capture: F,
    row_count: usize,
    error_count: usize,
    completed: bool,
}

impl<I, F> TracedResultIterator<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item),
{
    pub fn new(inner: I, span: Span, capture: F) -> Self {
        Self {
            inner,
            span,
            capture,
            row_count: 0,
            error_count: 0,
            completed: false,
        }
    }
}

impl<I, F, T, E> Iterator for TracedResultIterator<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(&Result<T, E>),
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let span = self.span.clone();
        span.in_scope(|| {
            let item = self.inner.next();

            match &item {
                Some(Ok(_)) => {
                    self.row_count += 1;
                }
                Some(Err(_)) => {
                    self.error_count += 1;
                }
                None => self.completed = true,
            }
            if let Some(item) = &item {
                (self.capture)(item);
            }

            item
        })
    }
}

impl<I, F> Drop for TracedResultIterator<I, F> {
    fn drop(&mut self) {
        self.span.record("row_count", self.row_count);
        self.span.record("error_count", self.error_count);
        self.span.record("completed", self.completed);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn installs_default_subscriber_once() {
        super::ensure_default_subscriber();
        super::ensure_default_subscriber();

        assert!(tracing::dispatcher::has_been_set());
    }
}
