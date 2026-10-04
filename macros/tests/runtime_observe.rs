use {
    gluesql_core::{observe, prelude::Glue},
    gluesql_macros::trace_storage,
    gluesql_memory_storage::MemoryStorage,
    std::sync::{Arc, Mutex},
    tracing::{
        Subscriber,
        span::{Attributes, Id},
    },
    tracing_subscriber::{
        Layer, Registry,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
    },
};

type Result<T> = std::result::Result<T, &'static str>;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<Vec<String>>>>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, _: &Id, _: Context<'_, S>) {
        assert!(attrs.metadata().fields().is_empty());
    }

    fn on_event(&self, _: &tracing::Event<'_>, _: Context<'_, S>) {
        self.0.lock().unwrap().push("event".into());
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).unwrap();
        self.1.lock().unwrap().push(
            span.scope()
                .from_root()
                .map(|span| span.name().to_owned())
                .collect(),
        );
        self.0.lock().unwrap().push(span.name().to_owned());
    }
}

#[observe]
fn automatic(early: bool) -> Result<u8> {
    if early {
        return Ok(1);
    }
    Err("failed")
}

#[observe]
fn borrow_input<'a>(key: &'a str) -> Result<&'a str> {
    Ok(key)
}

#[observe]
fn r#type() {}

#[observe]
async fn automatic_async() {
    std::future::pending::<()>().await;
}

struct MutableRows(Vec<u8>);
impl MutableRows {
    #[observe]
    fn rows_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }
}

#[cfg_attr(any(), observe)]
fn feature_off() -> u8 {
    7
}

#[test]
fn function_spans_preserve_returns_borrows_and_async_context() {
    let capture = Capture::default();
    tracing::subscriber::with_default(Registry::default().with(capture.clone()), || {
        let parent = tracing::info_span!("parent");
        let _entered = parent.enter();
        assert_eq!(automatic(true), Ok(1));
        assert_eq!(automatic(false), Err("failed"));
        assert_eq!(borrow_input("key"), Ok("key"));
        r#type();
        let mut rows = MutableRows(vec![1]);
        rows.rows_mut().push(2);
        assert_eq!(rows.0, vec![1, 2]);
        assert_eq!(feature_off(), 7);
        let mut future = Box::pin(automatic_async());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert_eq!(
            std::future::Future::poll(future.as_mut(), &mut context),
            std::task::Poll::Pending
        );
        assert_eq!(
            tracing::Span::current().metadata().unwrap().name(),
            "parent"
        );
        drop(future);
    });
    let records = capture.0.lock().unwrap();
    let names: Vec<_> = records.iter().map(String::as_str).collect();
    assert_eq!(
        names,
        [
            "automatic",
            "automatic",
            "borrow_input",
            "type",
            "rows_mut",
            "automatic_async",
            "parent"
        ]
    );
}

struct Opaque;
struct Storage(Vec<Opaque>);

#[trace_storage]
impl Storage {
    fn stream(&mut self) -> Result<Box<dyn Iterator<Item = &mut Opaque> + '_>> {
        Ok(Box::new(self.0.iter_mut()))
    }

    fn validate(&self) -> std::result::Result<(), Opaque> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Opaque)
        }
    }
}

#[test]
fn storage_spans_close_before_lazy_consumption_without_debug_bounds() {
    let capture = Capture::default();
    tracing::subscriber::with_default(Registry::default().with(capture.clone()), || {
        let mut storage = Storage(vec![Opaque, Opaque]);
        let rows = storage.stream().unwrap();
        assert_eq!(capture.0.lock().unwrap().len(), 1);
        assert_eq!(rows.count(), 2);
        assert!(storage.validate().is_err());
    });
    let records = capture.0.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], "gluesql.Storage.stream");
    assert_eq!(records[1], "gluesql.Storage.validate");
}

#[test]
fn query_pipeline_keeps_function_spans_without_stage_counters() {
    let capture = Capture::default();
    tracing::subscriber::with_default(Registry::default().with(capture.clone()), || {
        let mut glue = Glue::new(MemoryStorage::default());
        for sql in [
            "CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, code INTEGER UNIQUE)",
            "INSERT INTO items VALUES (1, 10, 101), (2, 10, 102), (3, 20, 103)",
            "SELECT DISTINCT category FROM items",
            "SELECT category, COUNT(*) FROM items GROUP BY category",
            "SELECT * FROM items ORDER BY id",
            "SELECT a.id FROM items a JOIN items b ON a.id = b.id",
            "UPDATE items SET category = 30 WHERE id = 1",
            "DELETE FROM items WHERE id = 2",
        ] {
            glue.execute(sql).unwrap();
        }
        glue.plan("SELECT * FROM items").unwrap();
    });
    let records = capture.0.lock().unwrap();
    for expected in [
        "execute_with_params",
        "parse",
        "translate_with_params",
        "plan",
        "plan_with_params",
        "execute",
        "sort",
        "build_rows",
        "fetch_rows",
        "validate_unique",
        "rows",
        "fetch",
        "collect_update_rows",
        "collect_keys",
    ] {
        assert!(
            records.iter().any(|name| name == expected),
            "missing {expected}"
        );
    }
    let paths = capture.1.lock().unwrap();
    for suffix in [
        vec!["execute", "execute", "execute"],
        vec!["execute", "execute", "sort"],
        vec!["execute", "collect_update_rows"],
        vec!["execute", "collect_keys"],
    ] {
        assert!(
            paths.iter().any(|path| path
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .ends_with(&suffix)),
            "missing hierarchy {suffix:?}"
        );
    }
    assert!(
        records
            .iter()
            .all(|name| !matches!(name.as_str(), "event" | "plan_statement" | "execute_stmt"))
    );
}

#[test]
fn disabled_subscriber_preserves_function_results() {
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        assert_eq!(automatic(true), Ok(1));
        assert_eq!(automatic(false), Err("failed"));
        assert_eq!(borrow_input("key"), Ok("key"));
    });
}
