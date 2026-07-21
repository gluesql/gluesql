use {super::FIXTURES, include_dir::Dir};

fn collect_fixture_paths(dir: &Dir<'_>, paths: &mut Vec<String>) {
    paths.extend(dir.files().filter_map(|file| {
        if file
            .path()
            .extension()
            .is_none_or(|extension| extension != "sql")
        {
            return None;
        }

        let path = file.path().to_string_lossy().replace('\\', "/");
        Some(path)
    }));

    for dir in dir.dirs() {
        collect_fixture_paths(dir, paths);
    }
}

fn registration_count(path: &str, source: &str) -> usize {
    let case = path
        .strip_suffix(".sql")
        .expect("fixture path should end with .sql")
        .replace('/', "::");
    let registration = format!("sql_case!({case});");

    source
        .lines()
        .filter(|line| line.trim() == registration)
        .count()
}

fn find_unregistered_fixtures(fixtures: &[String], source: &str) -> Vec<String> {
    fixtures
        .iter()
        .filter(|path| registration_count(path, source) == 0)
        .cloned()
        .collect()
}

fn find_duplicate_registrations(fixtures: &[String], source: &str) -> Vec<String> {
    fixtures
        .iter()
        .filter(|path| registration_count(path, source) > 1)
        .cloned()
        .collect()
}

#[test]
fn finds_unregistered_and_duplicate_fixtures() {
    let fixtures = vec![
        "basic.sql".to_owned(),
        "function/nullif.sql".to_owned(),
        "function/missing.sql".to_owned(),
    ];
    let source = r"
sql_case!(basic);
sql_case!(basic);
sql_case!(function::nullif);
";

    assert_eq!(
        find_unregistered_fixtures(&fixtures, source),
        vec!["function/missing.sql"]
    );
    assert_eq!(
        find_duplicate_registrations(&fixtures, source),
        vec!["basic.sql"]
    );
}

#[test]
fn all_fixtures_are_registered_once() {
    let mut fixtures = Vec::new();
    collect_fixture_paths(&FIXTURES, &mut fixtures);
    fixtures.sort();

    let source = include_str!("../lib.rs");
    let unregistered = find_unregistered_fixtures(&fixtures, source);
    let duplicates = find_duplicate_registrations(&fixtures, source);
    let mut errors = Vec::new();

    if !unregistered.is_empty() {
        errors.push(format!(
            "fixtures without sql_case! registration:\n  {}",
            unregistered.join("\n  ")
        ));
    }
    if !duplicates.is_empty() {
        errors.push(format!(
            "fixtures with duplicate sql_case! registrations:\n  {}",
            duplicates.join("\n  ")
        ));
    }

    assert!(
        errors.is_empty(),
        "fixture registration errors:\n{}",
        errors.join("\n")
    );
}
