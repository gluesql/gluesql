use {
    std::{
        fs,
        process::{Command, Output, Stdio},
    },
    tempfile::tempdir,
};

#[test]
fn config_validation_and_override_precedence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("gluesql.toml");
    let run = |extra: &[&str], env: &[(&str, &str)]| -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gluesql-cli"));
        command
            .current_dir(dir.path())
            .arg("--config")
            .arg(&path)
            .args(extra)
            .stdin(Stdio::null())
            .env_remove("RUST_LOG")
            .env_remove("GLUESQL_FLAMEGRAPH_PATH")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
        for (key, value) in env {
            command.env(key, value);
        }
        command.output().unwrap()
    };
    let succeeded = |output: Output| {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };

    fs::write(&path, "").unwrap();
    succeeded(run(&[], &[]));
    fs::write(&path, "[observability]\nunknown = true").unwrap();
    let output = run(&[], &[]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown field"));

    fs::write(&path, "[observability]\nfilter = 'invalid=level'").unwrap();
    assert!(!run(&[], &[]).status.success());
    if cfg!(feature = "tracing") {
        succeeded(run(&[], &[("RUST_LOG", "off")]));
        succeeded(run(
            &["--log-filter", "off"],
            &[("RUST_LOG", "invalid=level")],
        ));
    }

    fs::write(&path, "[observability.flamegraph]\npath = 'file.folded'").unwrap();
    if cfg!(feature = "tracing-flame") {
        succeeded(run(&[], &[]));
        assert!(dir.path().join("file.folded").exists());
        fs::remove_file(dir.path().join("file.folded")).unwrap();
        succeeded(run(&[], &[("GLUESQL_FLAMEGRAPH_PATH", "env.folded")]));
        assert!(dir.path().join("env.folded").exists());
        assert!(!dir.path().join("file.folded").exists());
        fs::remove_file(dir.path().join("env.folded")).unwrap();
        succeeded(run(
            &["--flamegraph-path", "cli.folded"],
            &[("GLUESQL_FLAMEGRAPH_PATH", "env.folded")],
        ));
        assert!(dir.path().join("cli.folded").exists());
        assert!(!dir.path().join("env.folded").exists());
    } else {
        let output = run(&[], &[]);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("requires the tracing-flame feature")
        );
    }

    fs::write(&path, "[observability.otlp]\nendpoint = 'invalid'").unwrap();
    assert!(!run(&[], &[]).status.success());
    if cfg!(feature = "opentelemetry") {
        succeeded(run(
            &[],
            &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")],
        ));
        succeeded(run(
            &[],
            &[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "invalid"),
                (
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                    "http://localhost:4318/custom",
                ),
            ],
        ));
        succeeded(run(
            &["--otlp-endpoint", "http://localhost:4318"],
            &[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "invalid")],
        ));
    } else {
        let output = run(&[], &[]);
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("requires the opentelemetry feature")
        );
    }
}
