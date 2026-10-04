use {
    anyhow::{Context, Result, ensure},
    serde::Deserialize,
    std::{
        env, fs,
        path::{Path, PathBuf},
    },
};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    observability: Observability,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Observability {
    pub filter: Option<String>,
    pub flamegraph: Option<Flamegraph>,
    pub otlp: Option<Otlp>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Flamegraph {
    pub path: Option<PathBuf>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Otlp {
    pub endpoint: Option<String>,
}

impl Observability {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration {}", path.display()))?;
        let config: Config = toml::from_str(&text)
            .with_context(|| format!("invalid configuration {}", path.display()))?;
        Ok(config.observability)
    }

    pub fn resolve(
        mut self,
        filter: Option<String>,
        flamegraph_path: Option<PathBuf>,
        otlp_endpoint: Option<String>,
    ) -> Result<Self> {
        self.filter = filter
            .or_else(|| {
                cfg!(feature = "tracing")
                    .then(|| env::var("RUST_LOG").ok())
                    .flatten()
            })
            .or(self.filter);
        let path = flamegraph_path.or_else(|| {
            cfg!(feature = "tracing-flame")
                .then(|| env::var_os("GLUESQL_FLAMEGRAPH_PATH").map(PathBuf::from))
                .flatten()
        });
        if let Some(path) = path {
            self.flamegraph.get_or_insert_with(Flamegraph::default).path = Some(path);
        }
        let endpoint = if let Some(endpoint) = otlp_endpoint {
            Some(trace_endpoint(&endpoint))
        } else if cfg!(feature = "opentelemetry") {
            env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
                .ok()
                .or_else(|| {
                    env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                        .ok()
                        .map(|endpoint| trace_endpoint(&endpoint))
                })
                .or_else(|| {
                    self.otlp
                        .as_ref()
                        .and_then(|config| config.endpoint.as_deref())
                        .map(trace_endpoint)
                })
        } else {
            self.otlp
                .as_ref()
                .and_then(|config| config.endpoint.clone())
        };
        if let Some(endpoint) = endpoint {
            self.otlp.get_or_insert_with(Otlp::default).endpoint = Some(endpoint);
        }
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            cfg!(feature = "tracing") || self.filter.is_none(),
            "observability.filter requires the tracing feature"
        );
        ensure!(
            cfg!(feature = "tracing-flame") || self.flamegraph.is_none(),
            "observability.flamegraph requires the tracing-flame feature"
        );
        ensure!(
            cfg!(feature = "opentelemetry") || self.otlp.is_none(),
            "observability.otlp requires the opentelemetry feature"
        );
        if let Some(filter) = &self.filter {
            ensure!(
                !filter.trim().is_empty(),
                "observability.filter must not be empty"
            );
        }
        if let Some(path) = self
            .flamegraph
            .as_ref()
            .and_then(|config| config.path.as_ref())
        {
            ensure!(
                !path.as_os_str().is_empty(),
                "observability.flamegraph.path must not be empty"
            );
        }
        if let Some(endpoint) = self
            .otlp
            .as_ref()
            .and_then(|config| config.endpoint.as_ref())
        {
            ensure!(
                endpoint.starts_with("http://") || endpoint.starts_with("https://"),
                "observability.otlp.endpoint must be an HTTP or HTTPS URL"
            );
        }
        Ok(())
    }
}

fn trace_endpoint(base: &str) -> String {
    format!("{}/v1/traces", base.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::{Config, Observability, trace_endpoint};

    #[test]
    fn parse_observability_and_reject_unknown_settings() {
        let config: Config = toml::from_str(
            r#"
            [observability]
            filter = "gluesql=debug"
            [observability.flamegraph]
            path = "query.folded"
            [observability.otlp]
            endpoint = "http://localhost:4318"
        "#,
        )
        .unwrap();
        assert_eq!(
            config.observability.filter.as_deref(),
            Some("gluesql=debug")
        );
        assert!(toml::from_str::<Config>("[observability]\nfilters = 'debug'").is_err());
        assert!(
            toml::from_str::<Config>("[observability.otlp]\nendpont = 'http://localhost'").is_err()
        );
        assert_eq!(
            trace_endpoint("http://localhost:4318/"),
            "http://localhost:4318/v1/traces"
        );
    }

    #[test]
    fn reject_exporters_missing_from_build() {
        let config: Config = toml::from_str("[observability.flamegraph]").unwrap();
        assert_eq!(
            config.observability.validate().is_ok(),
            cfg!(feature = "tracing-flame")
        );
        let config: Config = toml::from_str("[observability.otlp]").unwrap();
        assert_eq!(
            config.observability.validate().is_ok(),
            cfg!(feature = "opentelemetry")
        );
        assert!(
            Observability::load(Some(std::path::Path::new("/nonexistent/gluesql.toml"))).is_err()
        );
    }
}
