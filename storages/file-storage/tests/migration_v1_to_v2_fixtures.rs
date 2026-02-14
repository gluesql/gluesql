use {
    gluesql_file_storage::{FileStorage, migrate_to_latest},
    std::{
        collections::BTreeMap,
        fs::{copy, create_dir, create_dir_all, read_dir, read_to_string, remove_dir_all},
        path::Path,
    },
    uuid::Uuid,
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

#[tokio::test]
async fn migrate_v1_mixed_schema_schemaless_fixture() {
    let fixture_name = "mixed_schema_schemaless";
    let (path, _guard) = fixture_to_tmp(fixture_name);

    let first = migrate_to_latest(&path).expect("migrate fixture");
    assert_eq!(first.migrated_tables, 2);
    assert_eq!(first.unchanged_tables, 0);
    assert_eq!(first.rewritten_rows, 4);

    let second = migrate_to_latest(&path).expect("migrate fixture twice");
    assert_eq!(second.migrated_tables, 0);
    assert_eq!(second.unchanged_tables, 2);
    assert_eq!(second.rewritten_rows, 0);

    FileStorage::new(&path).expect("open migrated storage");

    let actual = collect_tree_files(Path::new(&path));
    let expected = collect_tree_files(Path::new(&fixture_case_path(fixture_name, "expected")));
    assert_eq!(actual, expected);
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

        create_dir_all(&target_path).expect("create fixture table directory");
        for child in read_dir(&source_path).expect("read fixture table directory") {
            let child = child.expect("fixture table directory entry");
            let child_type = child.file_type().expect("fixture file type");
            let child_source = child.path();
            let child_target = target_path.join(child.file_name());

            assert!(
                child_type.is_file(),
                "unexpected nested fixture entry (depth > 2): {}",
                child_source.display()
            );

            copy(&child_source, &child_target).expect("copy fixture row file");
        }
    }
}

fn collect_tree_files(path: &Path) -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();
    for entry in read_dir(path).expect("read directory") {
        let entry = entry.expect("directory entry");
        let file_type = entry.file_type().expect("file type");
        let entry_path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if file_type.is_file() {
            let data = read_to_string(&entry_path).expect("read file");
            let data = data.replace("\r\n", "\n");
            files.insert(name.into_owned(), data);
            continue;
        }

        assert!(
            file_type.is_dir(),
            "unexpected fixture entry type: {}",
            entry_path.display()
        );

        for child in read_dir(&entry_path).expect("read fixture table directory") {
            let child = child.expect("fixture table directory entry");
            let child_type = child.file_type().expect("fixture file type");
            let child_path = child.path();
            let child_name = child.file_name();
            let child_name = child_name.to_string_lossy();

            assert!(
                child_type.is_file(),
                "unexpected nested fixture entry (depth > 2): {}",
                child_path.display()
            );

            let relative = format!("{name}/{child_name}");
            let data = read_to_string(&child_path).expect("read file");
            let data = data.replace("\r\n", "\n");
            files.insert(relative, data);
        }
    }

    files
}

fn fixture_case_path(fixture_name: &str, role: &str) -> String {
    format!("./tests/fixtures/v1_to_v2/{fixture_name}/{role}")
}

fn fixture_to_tmp(fixture_name: &str) -> (String, DirGuard) {
    let _ = create_dir("tmp");

    let source = fixture_case_path(fixture_name, "actual");
    let target = format!("./tmp/{fixture_name}-{}", Uuid::now_v7());
    copy_fixture_tree(Path::new(&source), Path::new(&target));

    (target.clone(), DirGuard { path: target })
}
