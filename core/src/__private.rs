use {
    std::sync::OnceLock,
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

#[cfg(test)]
mod tests {
    #[test]
    fn installs_default_subscriber_once() {
        super::ensure_default_subscriber();
        super::ensure_default_subscriber();

        assert!(tracing::dispatcher::has_been_set());
    }
}
