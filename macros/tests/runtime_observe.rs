use {
    gluesql_core::prelude::Glue,
    gluesql_macros::observe,
    gluesql_memory_storage::MemoryStorage,
    std::{
        collections::BTreeMap,
        fmt,
        sync::atomic::{AtomicUsize, Ordering},
        sync::{Arc, Mutex},
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
struct Capture(Arc<Mutex<CapturedSpans>>);

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

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).unwrap();
        let values = span.extensions().get::<Values>().unwrap().0.clone();
        self.0
            .lock()
            .unwrap()
            .push((span.name().to_owned(), values));
    }
}

#[observe(name = "bindings", fields(kind = "test"), after_let(rows, occurrence = 2, record(buffered_rows = rows.len())))]
fn bindings() -> Vec<usize> {
    let rows = vec![1, 1, 2];
    let rows = rows.into_iter().collect::<Vec<_>>();
    rows.into_iter().filter(|n| *n == 2).collect()
}

#[observe(name = "branches", after_let(rows, all, record(n = rows.len())))]
fn branches(left: bool) -> Vec<u8> {
    if left {
        let rows = vec![1];
        rows
    } else {
        let rows = vec![2, 3];
        rows
    }
}

#[observe(name = "range", start = before_let(rows), end = after_let(n), record(n = n))]
fn range(fail: bool) -> Result<usize> {
    assert_eq!(
        tracing::Span::current().metadata().unwrap().name(),
        "parent"
    );
    let rows = [1, 2];
    assert_eq!(tracing::Span::current().metadata().unwrap().name(), "range");
    if fail {
        return Err("failed");
    }
    let n = rows.len();
    assert_eq!(
        tracing::Span::current().metadata().unwrap().name(),
        "parent"
    );
    Ok(n)
}

#[observe(name = "scan", count_loop(binding = row, increment = after_let(value), field = scanned_rows))]
fn scan(rows: Vec<Result<u8>>, stop: bool) -> Result<()> {
    for row in rows {
        let value = row?;
        if stop && value == 2 {
            break;
        }
    }
    Ok(())
}

#[observe(name = "entries", count_loop(binding = row, field = entries), after_loop(row, record(done = true)))]
fn entries() -> Result<()> {
    for row in [Ok(1), Err("bad")] {
        row?;
    }
    Ok(())
}

#[observe(name = "result", on_ok(value, record(n = value.len())))]
fn result(early: bool, fail: bool) -> Result<Vec<usize>> {
    if fail {
        return Err("failed");
    }
    if early {
        return Ok(vec![1]);
    }
    Ok(vec![1, 2])
}

struct Example;
impl Example {
    #[observe(name = "method", fields(key = ?key), on_ok(value, record(n = value.len())))]
    fn method<'a>(&self, key: &'a str) -> Result<&'a str> {
        Ok(key)
    }
}

#[test]
fn records_values_without_changing_control_flow() {
    let capture = Capture::default();
    let subscriber = Registry::default().with(capture.clone());
    tracing::subscriber::with_default(subscriber, || {
        let parent = tracing::info_span!("parent");
        let _entered = parent.enter();
        assert_eq!(bindings(), vec![2]);
        assert_eq!(branches(true), vec![1]);
        assert_eq!(branches(false), vec![2, 3]);
        assert_eq!(range(false), Ok(2));
        assert_eq!(range(true), Err("failed"));
        assert_eq!(
            tracing::Span::current().metadata().unwrap().name(),
            "parent"
        );
        assert_eq!(scan(vec![Ok(1), Ok(2), Err("bad")], false), Err("bad"));
        assert_eq!(scan(vec![Ok(1), Ok(2), Ok(3)], true), Ok(()));
        assert_eq!(entries(), Err("bad"));
        assert_eq!(result(false, false), Ok(vec![1, 2]));
        assert_eq!(result(true, false), Ok(vec![1]));
        assert_eq!(result(false, true), Err("failed"));
        assert_eq!(Example.method("key"), Ok("key"));
    });
    let spans = capture.0.lock().unwrap();
    let values = |name: &str, field: &str| -> Vec<Option<String>> {
        spans
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, v)| v.get(field).cloned())
            .collect()
    };
    assert_eq!(values("bindings", "buffered_rows"), vec![Some("3".into())]);
    assert_eq!(
        values("branches", "n"),
        vec![Some("1".into()), Some("2".into())]
    );
    assert_eq!(values("range", "n"), vec![Some("2".into()), None]);
    assert_eq!(
        values("scan", "scanned_rows"),
        vec![Some("2".into()), Some("2".into())]
    );
    assert_eq!(values("entries", "entries"), vec![Some("2".into())]);
    assert_eq!(values("entries", "done"), vec![None]);
    assert_eq!(
        values("result", "n"),
        vec![Some("2".into()), Some("1".into()), None]
    );
    assert_eq!(values("method", "n"), vec![Some("3".into())]);
}

static EVALUATIONS: AtomicUsize = AtomicUsize::new(0);

#[observe(name = "disabled", fields(n = EVALUATIONS.fetch_add(1, Ordering::SeqCst)), after_let(rows, record(n = EVALUATIONS.fetch_add(rows.len(), Ordering::SeqCst))))]
fn disabled() {
    let rows = [1];
    assert_eq!(rows.len(), 1);
}

#[test]
fn disabled_subscriber_does_not_evaluate_fields() {
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), disabled);
    assert_eq!(EVALUATIONS.load(Ordering::SeqCst), 0);
}

#[cfg_attr(any(), gluesql_macros::observe(name = "off", after_let(missing, record(n = nonexistent()))))]
fn feature_off() -> u8 {
    7
}

#[test]
fn disabled_attribute_keeps_original_function() {
    assert_eq!(feature_off(), 7);
}

#[test]
fn query_pipeline_preserves_measurement_points() {
    let capture = Capture::default();
    tracing::subscriber::with_default(Registry::default().with(capture.clone()), || {
        let mut glue = Glue::new(MemoryStorage::default());
        glue.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, code INTEGER UNIQUE)",
        )
        .unwrap();
        glue.execute("INSERT INTO items VALUES (1, 10, 101), (2, 10, 102), (3, 20, 103)")
            .unwrap();
        glue.execute("SELECT DISTINCT category FROM items").unwrap();
        glue.execute("SELECT category, COUNT(*) FROM items GROUP BY category")
            .unwrap();
        glue.execute("SELECT * FROM items ORDER BY id").unwrap();
        glue.execute("SELECT a.id FROM items a JOIN items b ON a.id = b.id")
            .unwrap();
        glue.execute("INSERT INTO items VALUES (4, 20, 104)")
            .unwrap();
        glue.execute("UPDATE items SET category = 30 WHERE id = 1")
            .unwrap();
        glue.execute("DELETE FROM items WHERE id = 2").unwrap();
        glue.execute("CREATE TABLE documents").unwrap();
        glue.execute("INSERT INTO documents VALUES ('{\"name\":\"Tachibana Sherry\"}')")
            .unwrap();
        glue.execute("SELECT * FROM documents").unwrap();
    });
    let spans = capture.0.lock().unwrap();
    let contains = |name: &str, field: &str, value: &str| {
        spans
            .iter()
            .any(|(n, fields)| n == name && fields.get(field).is_some_and(|v| v == value))
    };
    assert!(contains("gluesql.query.distinct", "buffered_rows", "3"));
    assert!(contains("gluesql.query.aggregate", "buffered_groups", "2"));
    assert!(contains("gluesql.query.order_by", "buffered_rows", "3"));
    assert!(contains(
        "gluesql.query.hash_join.build",
        "buffered_rows",
        "3"
    ));
    assert!(contains("gluesql.insert.collect", "buffered_rows", "3"));
    assert!(contains("gluesql.insert.collect", "buffered_rows", "1"));
    assert!(contains("gluesql.result.materialize", "buffered_rows", "2"));
    assert!(contains("gluesql.result.materialize", "buffered_rows", "1"));
    assert!(contains("gluesql.validate.unique", "scanned_rows", "3"));
    let mutations: Vec<_> = spans
        .iter()
        .filter(|(n, _)| n == "gluesql.mutation.collect")
        .collect();
    assert_eq!(mutations.len(), 2);
    assert!(
        mutations
            .iter()
            .all(|(_, fields)| fields.get("buffered_rows") == Some(&"1".to_owned()))
    );
    assert!(contains(
        "gluesql.execute",
        "sql",
        "SELECT DISTINCT category FROM items"
    ));
}
