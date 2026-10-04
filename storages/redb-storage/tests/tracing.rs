#![cfg(feature = "tracing")]

use {
    gluesql_core::{prelude::Glue, store::Planner},
    gluesql_redb_storage::RedbStorage,
    std::sync::{Arc, Mutex},
    tracing::{Subscriber, span::Id},
    tracing_subscriber::{
        Layer, Registry,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
    },
};

struct Capture(Arc<Mutex<Vec<Vec<String>>>>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        self.0.lock().unwrap().push(
            ctx.span(&id)
                .unwrap()
                .scope()
                .from_root()
                .map(|span| span.name().to_owned())
                .collect(),
        );
    }
}

#[test]
fn observes_actual_implementations_without_delegating_wrapper_spans() {
    let path = std::env::temp_dir().join(format!("gluesql-tracing-{}.redb", uuid::Uuid::now_v7()));
    let paths = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::with_default(
        Registry::default().with(Capture(Arc::clone(&paths))),
        || {
            let mut glue = Glue::new(RedbStorage::new(&path).unwrap());
            glue.execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY);
            INSERT INTO items VALUES (1), (2);
            SELECT * FROM items WHERE id = 1;
            SELECT * FROM items ORDER BY id;",
            )
            .unwrap();
            let statement = glue.plan("SELECT * FROM items").unwrap().remove(0);
            glue.storage.plan(statement).unwrap();
        },
    );
    std::fs::remove_file(path).unwrap();

    let paths = paths.lock().unwrap();
    for method in [
        "fetch_schema",
        "insert_schema",
        "insert_data",
        "fetch_data",
        "scan_data",
        "begin",
        "commit",
    ] {
        let name = method.to_owned();
        assert!(
            paths.iter().any(|path| path.last() == Some(&name)),
            "missing {name}"
        );
    }
    for stage in ["plan", "execute"] {
        assert_eq!(
            paths
                .iter()
                .filter(|path| path.len() == 2
                    && path[0] == "execute_with_params"
                    && path[1] == stage)
                .count(),
            4
        );
    }
    assert!(paths.iter().any(|path| path.as_slice() == ["plan"]));
    assert!(paths.iter().flatten().all(|name| name != "plan_statement"
        && name != "execute_stmt"
        && !name.starts_with("gluesql.RedbStorage.")));
}
