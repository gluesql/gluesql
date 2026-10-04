use {
    gluesql_core::{observe, prelude::Glue},
    gluesql_macros::trace_storage,
    gluesql_memory_storage::MemoryStorage,
    std::{
        collections::BTreeMap,
        fmt,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    },
    tracing::{
        Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
    },
    tracing_subscriber::{
        Layer, Registry,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
    },
};

type Result<T> = std::result::Result<T, &'static str>;
type CapturedSpans = Vec<(String, BTreeMap<String, String>)>;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<CapturedSpans>>, Arc<Mutex<Vec<Vec<String>>>>);

#[derive(Default)]
struct Values(BTreeMap<String, String>);

impl Visit for Values {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut values = Values::default();
        attrs.record(&mut values);
        ctx.span(id).unwrap().extensions_mut().insert(values);
    }

    fn on_record(&self, id: &Id, record: &Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        record.record(span.extensions_mut().get_mut::<Values>().unwrap());
    }

    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut values = Values::default();
        event.record(&mut values);
        self.0.lock().unwrap().push(("event".into(), values.0));
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).unwrap();
        let values = span.extensions().get::<Values>().unwrap().0.clone();
        self.1.lock().unwrap().push(
            span.scope()
                .from_root()
                .map(|span| span.name().to_owned())
                .collect(),
        );
        self.0
            .lock()
            .unwrap()
            .push((span.name().to_owned(), values));
    }
}

#[observe]
fn automatic(early: bool) -> Result<u8> {
    if early {
        return Ok(1);
    }
    Err("failed")
}

#[observe(name = "custom", fields(key = %key))]
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

#[cfg_attr(any(), observe(fields(n = nonexistent())))]
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
    let names: Vec<_> = records.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "automatic",
            "automatic",
            "custom",
            "type",
            "rows_mut",
            "automatic_async",
            "parent"
        ]
    );
    let (_, fields) = records.iter().find(|(name, _)| name == "custom").unwrap();
    assert_eq!(fields.get("key").map(String::as_str), Some("key"));
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
    assert_eq!(records[0].0, "gluesql.Storage.stream");
    assert_eq!(records[1].0, "gluesql.Storage.validate");
    assert!(records.iter().all(|(_, fields)| fields.is_empty()));
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
    });
    let records = capture.0.lock().unwrap();
    for expected in [
        "gluesql.execute",
        "gluesql.parse",
        "gluesql.translate",
        "gluesql.plan",
        "gluesql.execute_statement",
        "execute",
        "sort",
        "build_rows",
        "fetch_rows",
        "validate_unique",
        "rows",
    ] {
        assert!(
            records.iter().any(|(name, _)| name == expected),
            "missing {expected}"
        );
    }
    for (function, operation) in [
        ("collect_update_rows", "update"),
        ("collect_keys", "delete"),
    ] {
        assert!(records.iter().any(|(name, fields)| {
            name == function
                && fields
                    .get("operation")
                    .is_some_and(|value| value == &format!("{operation:?}"))
        }));
    }
    let paths = capture.1.lock().unwrap();
    for suffix in [
        vec!["gluesql.execute_statement", "execute", "execute"],
        vec!["gluesql.execute_statement", "execute", "sort"],
        vec!["gluesql.execute_statement", "collect_update_rows"],
        vec!["gluesql.execute_statement", "collect_keys"],
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
    assert!(records.iter().all(|(name, fields)| {
        name != "event"
            && ["buffered_rows", "buffered_groups", "scanned_rows", "params"]
                .iter()
                .all(|key| !fields.contains_key(*key))
    }));
}

#[test]
fn disabled_subscriber_does_not_evaluate_entry_fields() {
    static EVALUATIONS: AtomicUsize = AtomicUsize::new(0);
    #[observe(fields(n = EVALUATIONS.fetch_add(1, Ordering::SeqCst)))]
    fn disabled() {}
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), disabled);
    assert_eq!(EVALUATIONS.load(Ordering::SeqCst), 0);
}
