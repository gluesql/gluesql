use {
    gluesql_core::prelude::{
        Glue, Payload,
        Value::{Bool, I64, List, Map, Str},
    },
    gluesql_sled_storage::{SledStorage, migrate_to_latest},
    std::{
        collections::BTreeMap,
        fs::{copy, create_dir, create_dir_all, read_dir, remove_dir_all},
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    },
};

struct DirGuard {
    path: String,
}

impl Drop for DirGuard {
    fn drop(&mut self) {
        if let Err(err) = remove_dir_all(&self.path) {
            eprintln!("remove_dir_all error: {err:?}");
        }
    }
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos()
}

fn fixture_to_tmp(fixture_name: &str) -> (String, DirGuard) {
    let _ = create_dir("tmp");

    let source = format!("./tests/fixtures/v1/{fixture_name}.sled");
    let target = format!("./tmp/{fixture_name}-{}", unique_suffix());
    copy_fixture_tree(Path::new(&source), Path::new(&target));

    (target.clone(), DirGuard { path: target })
}

fn copy_fixture_tree(source: &Path, target: &Path) {
    create_dir_all(target).expect("create target directory");

    for entry in read_dir(source).expect("read source directory") {
        let entry = entry.expect("directory entry");
        let file_type = entry.file_type().expect("file type");
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());

        if file_type.is_file() {
            copy(&source_path, &target_path).expect("copy fixture file");
            continue;
        }

        assert!(
            file_type.is_dir(),
            "unexpected fixture entry type: {}",
            source_path.display()
        );

        create_dir_all(&target_path).expect("create fixture child directory");
        for child in read_dir(&source_path).expect("read fixture child directory") {
            let child = child.expect("fixture child entry");
            let child_type = child.file_type().expect("child file type");
            let child_source = child.path();
            let child_target = target_path.join(child.file_name());

            assert!(
                child_type.is_file(),
                "unexpected nested fixture entry (depth > 2): {}",
                child_source.display()
            );

            copy(&child_source, &child_target).expect("copy fixture child file");
        }
    }
}

#[tokio::test]
async fn migrate_v1_mixed_schema_schemaless_fixture() {
    let (path, _guard) = fixture_to_tmp("mixed_schema_schemaless");

    let first = migrate_to_latest(&path).expect("migrate fixture");
    assert_eq!(first.migrated_tables, 2);
    assert_eq!(first.unchanged_tables, 0);
    assert_eq!(first.rewritten_rows, 4);

    let second = migrate_to_latest(&path).expect("migrate fixture twice");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 2);
    assert_eq!(second.rewritten_rows, 0);

    let storage = SledStorage::new(&path).expect("open migrated storage");
    let mut glue = Glue::new(storage);

    let user_rows = glue
        .execute("SELECT id, name, active FROM User ORDER BY id;")
        .await
        .expect("select User");
    assert_eq!(
        user_rows,
        vec![Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned(), "active".to_owned()],
            rows: vec![
                vec![I64(1), Str("Alice".to_owned()), Bool(true)],
                vec![I64(2), Str("Bob".to_owned()), Bool(false)],
            ],
        }],
    );

    let inserted_user = glue
        .execute("INSERT INTO User VALUES (3, 'Carol', TRUE);")
        .await
        .expect("insert User row");
    assert_eq!(inserted_user, vec![Payload::Insert(1)]);

    let user_count = glue
        .execute("SELECT COUNT(*) AS cnt FROM User;")
        .await
        .expect("count User rows");
    assert_eq!(
        user_count,
        vec![Payload::Select {
            labels: vec!["cnt".to_owned()],
            rows: vec![vec![I64(3)]],
        }],
    );

    let event_query = glue
        .execute("SELECT kind, meta['ip'] AS ip FROM Event WHERE event_id = 1;")
        .await
        .expect("select Event projection");
    assert_eq!(
        event_query,
        vec![Payload::Select {
            labels: vec!["kind".to_owned(), "ip".to_owned()],
            rows: vec![vec![Str("login".to_owned()), Str("10.0.0.1".to_owned())]],
        }],
    );

    let inserted_event = glue
        .execute(
            "INSERT INTO Event VALUES ('{\"event_id\":3,\"kind\":\"logout\",\"meta\":{\"ip\":\"10.0.0.3\"}}');",
        )
        .await
        .expect("insert Event row");
    assert_eq!(inserted_event, vec![Payload::Insert(1)]);

    let inserted_event_kind = glue
        .execute("SELECT kind FROM Event WHERE event_id = 3;")
        .await
        .expect("select inserted Event");
    assert_eq!(
        inserted_event_kind,
        vec![Payload::Select {
            labels: vec!["kind".to_owned()],
            rows: vec![vec![Str("logout".to_owned())]],
        }],
    );

    let event_map_rows = glue
        .execute("SELECT * FROM Event WHERE event_id = 1;")
        .await
        .expect("select schemaless Event rows");
    assert_eq!(
        event_map_rows,
        vec![Payload::SelectMap(vec![BTreeMap::from([
            ("event_id".to_owned(), I64(1)),
            ("kind".to_owned(), Str("login".to_owned())),
            (
                "meta".to_owned(),
                Map(BTreeMap::from([(
                    "ip".to_owned(),
                    Str("10.0.0.1".to_owned())
                )])),
            ),
            (
                "tags".to_owned(),
                List(vec![Str("auth".to_owned()), Str("web".to_owned())]),
            ),
        ])])],
    );
}
