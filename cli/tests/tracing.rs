use {
    std::{
        fs,
        process::{Command, Output, Stdio},
    },
    tempfile::tempdir,
};

fn run(directory: &std::path::Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_gluesql-cli"));
    command
        .current_dir(directory)
        .args(args)
        .stdin(Stdio::null())
        .env_remove("RUST_LOG")
        .env_remove("GLUESQL_FLAMEGRAPH_PATH")
        .env_remove("GLUESQL_FIREFOX_PROFILE_PATH")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().unwrap()
}

#[cfg(feature = "firefox-profile")]
#[test]
fn firefox_profile_is_saved_on_exit_with_query_and_storage_markers() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join("query.sql"),
        "CREATE TABLE items (id INTEGER PRIMARY KEY);
         INSERT INTO items VALUES (1);
         SELECT * FROM items;",
    )
    .unwrap();
    let output = run(
        dir.path(),
        &[
            "--storage",
            "redb",
            "--path",
            "query.redb",
            "--execute",
            "query.sql",
        ],
        &[("GLUESQL_FIREFOX_PROFILE_PATH", "query.json")],
    );
    assert!(output.status.success());
    let profile: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join("query.json")).unwrap()).unwrap();
    let thread = &profile["threads"][0];
    let strings = thread["stringArray"].as_array().unwrap();
    for name in ["execute_with_params", "plan", "scan_data", "commit"] {
        assert!(strings.iter().any(|value| value == name), "missing {name}");
    }
    assert!(!thread["markers"]["data"].as_array().unwrap().is_empty());

    let output = run(dir.path(), &[], &[("RUST_LOG", "off")]);
    assert!(output.status.success());
    let profile: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join("gluesql-profile.json")).unwrap())
            .unwrap();
    assert!(
        profile["threads"][0]["markers"]["data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !run(dir.path(), &[], &[("GLUESQL_FIREFOX_PROFILE_PATH", "")])
            .status
            .success()
    );
}

#[test]
fn environment_controls_tracing_outputs() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("query.sql"), "SELECT 1;").unwrap();
    let output = run(dir.path(), &["--execute", "query.sql"], &[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    if cfg!(feature = "tracing") {
        assert!(stderr.contains("execute_with_params"), "{stderr}");
        assert!(!stderr.contains("sql="), "{stderr}");
        let output = run(
            dir.path(),
            &["--execute", "query.sql"],
            &[("RUST_LOG", "off")],
        );
        assert!(output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("execute_with_params"));
        assert!(
            !run(dir.path(), &[], &[("RUST_LOG", "invalid=level")])
                .status
                .success()
        );
    }
    if cfg!(feature = "tracing-flame") {
        assert!(dir.path().join("tracing.folded").exists());
        let output = run(
            dir.path(),
            &[],
            &[("GLUESQL_FLAMEGRAPH_PATH", "query.folded")],
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(dir.path().join("query.folded").exists());
        assert!(
            !run(dir.path(), &[], &[("GLUESQL_FLAMEGRAPH_PATH", "")])
                .status
                .success()
        );
    }
}
