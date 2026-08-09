use {
    fxprof_processed_profile::{
        CategoryHandle, CounterHandle, MarkerFieldFlags, MarkerFieldFormat, MarkerLocations,
        MarkerTiming, ProcessHandle, Profile, SamplingInterval, StaticSchemaMarker,
        StaticSchemaMarkerField, StringHandle, ThreadHandle, Timestamp,
    },
    std::{
        env,
        error::Error,
        fs::File,
        io::BufWriter,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::{Duration, Instant, SystemTime},
    },
    tracing::{Event, Subscriber, field::Visit, span},
    tracing_subscriber::{
        EnvFilter, Layer, fmt,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
        util::SubscriberInitExt,
    },
};

#[derive(Clone)]
pub struct FirefoxProfileLayer {
    state: Arc<Mutex<ProfileState>>,
}

struct ProfileState {
    profile: Profile,
    process: ProcessHandle,
    thread: ThreadHandle,
    rss_counter: CounterHandle,
    started_at: Instant,
    previous_rss: u64,
    path: PathBuf,
}

struct SpanRecord {
    name: &'static str,
    started_at: Instant,
    fields: FieldVisitor,
}

#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    rss_bytes: Option<u64>,
    fields: Vec<String>,
}

struct TraceMarker {
    name: StringHandle,
    fields: StringHandle,
}

impl StaticSchemaMarker for TraceMarker {
    const UNIQUE_MARKER_TYPE_NAME: &'static str = "GlueSQL tracing";
    const LOCATIONS: MarkerLocations = MarkerLocations::MARKER_CHART
        .union(MarkerLocations::MARKER_TABLE)
        .union(MarkerLocations::TIMELINE_OVERVIEW);
    const CHART_LABEL: Option<&'static str> = Some("{marker.name}");
    const TABLE_LABEL: Option<&'static str> = Some("{marker.name}");
    const FIELDS: &'static [StaticSchemaMarkerField] = &[StaticSchemaMarkerField {
        key: "fields",
        label: "Fields",
        format: MarkerFieldFormat::String,
        flags: MarkerFieldFlags::SEARCHABLE,
    }];

    fn name(&self, _profile: &mut Profile) -> StringHandle {
        self.name
    }

    fn category(&self, _profile: &mut Profile) -> CategoryHandle {
        CategoryHandle::OTHER
    }

    fn string_field_value(&self, _field_index: u32) -> StringHandle {
        self.fields
    }

    fn number_field_value(&self, _field_index: u32) -> f64 {
        unreachable!()
    }
}

impl FirefoxProfileLayer {
    fn new(path: PathBuf) -> Self {
        let started_at = Instant::now();
        let mut profile = Profile::new(
            "GlueSQL resource benchmark",
            SystemTime::now().into(),
            SamplingInterval::from_millis(1),
        );
        let start = Timestamp::from_nanos_since_reference(0);
        let pid = std::process::id();
        let process = profile.add_process("resource_benchmark", pid, start);
        let thread = profile.add_thread(process, pid, start, true);
        profile.set_thread_name(thread, "GlueSQL");
        profile.add_initial_visible_thread(thread);
        profile.add_initial_selected_thread(thread);
        profile.set_symbolicated(true);
        let rss_counter = profile.add_counter(
            process,
            "process_rss",
            "Memory",
            "Resident set size in bytes",
        );

        Self {
            state: Arc::new(Mutex::new(ProfileState {
                profile,
                process,
                thread,
                rss_counter,
                started_at,
                previous_rss: 0,
                path,
            })),
        }
    }

    pub fn finish(&self) -> Result<(), Box<dyn Error>> {
        let mut state = self.state.lock().expect("Firefox profile lock poisoned");
        let end = timestamp(state.started_at.elapsed());
        let process = state.process;
        let thread = state.thread;
        state.profile.set_process_end_time(process, end);
        state.profile.set_thread_end_time(thread, end);
        serde_json::to_writer(BufWriter::new(File::create(&state.path)?), &state.profile)?;
        Ok(())
    }

    fn add_marker(&self, name: &str, fields: &str, timing: MarkerTiming) {
        let mut state = self.state.lock().expect("Firefox profile lock poisoned");
        let name = state.profile.intern_string(name);
        let fields = state.profile.intern_string(fields);
        let thread = state.thread;
        state
            .profile
            .add_marker(thread, timing, TraceMarker { name, fields });
    }

    fn now(&self) -> Timestamp {
        let state = self.state.lock().expect("Firefox profile lock poisoned");
        timestamp(state.started_at.elapsed())
    }
}

impl<S> Layer<S> for FirefoxProfileLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut fields = FieldVisitor::default();
        attrs.record(&mut fields);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanRecord {
                name: attrs.metadata().name(),
                started_at: Instant::now(),
                fields,
            });
        }
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(record) = span.extensions_mut().get_mut::<SpanRecord>()
        {
            values.record(&mut record.fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = FieldVisitor::default();
        event.record(&mut fields);
        if let Some(rss_bytes) = fields.rss_bytes {
            let mut state = self.state.lock().expect("Firefox profile lock poisoned");
            let sample_time = timestamp(state.started_at.elapsed());
            let delta = rss_bytes as f64 - state.previous_rss as f64;
            state.previous_rss = rss_bytes;
            let counter = state.rss_counter;
            state
                .profile
                .add_counter_sample(counter, sample_time, delta, 0);
            return;
        }

        let name = fields
            .message
            .as_deref()
            .unwrap_or_else(|| event.metadata().name());
        self.add_marker(
            name,
            &fields.fields.join(", "),
            MarkerTiming::Instant(self.now()),
        );
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id)
            && let Some(record) = span.extensions_mut().remove::<SpanRecord>()
        {
            let state = self.state.lock().expect("Firefox profile lock poisoned");
            let start = timestamp(record.started_at.duration_since(state.started_at));
            let end = timestamp(state.started_at.elapsed());
            drop(state);
            self.add_marker(
                record.name,
                &record.fields.fields.join(", "),
                MarkerTiming::Interval(start, end),
            );
        }
    }
}

impl Visit for FieldVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "rss_bytes" {
            self.rss_bytes = Some(value);
        }
        self.fields.push(format!("{}={value}", field.name()));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        } else {
            self.fields.push(format!("{}={value}", field.name()));
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        } else {
            self.fields.push(format!("{}={value:?}", field.name()));
        }
    }
}

fn timestamp(elapsed: Duration) -> Timestamp {
    Timestamp::from_nanos_since_reference(elapsed.as_nanos() as u64)
}

pub fn init() -> Result<FirefoxProfileLayer, Box<dyn Error>> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("gluesql=info"));
    let layer = FirefoxProfileLayer::new(
        env::var_os("GLUESQL_FIREFOX_PROFILE_PATH")
            .unwrap_or_else(|| "gluesql-benchmark-profile.json".into())
            .into(),
    );

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_span_events(super::FmtSpan::CLOSE)
                .with_writer(super::io::stderr),
        )
        .with(layer.clone())
        .try_init()?;

    Ok(layer)
}

#[cfg(test)]
mod tests {
    use {
        super::{FirefoxProfileLayer, MarkerTiming, Timestamp},
        serde_json::Value,
        std::path::PathBuf,
    };

    #[test]
    fn profile_contains_tracing_markers_and_rss_counter() {
        let layer = FirefoxProfileLayer::new(PathBuf::new());
        layer.add_marker(
            "gluesql.execute",
            "rows=1",
            MarkerTiming::Instant(Timestamp::from_nanos_since_reference(1)),
        );

        let state = layer.state.lock().expect("Firefox profile lock poisoned");
        let profile = serde_json::to_value(&state.profile).expect("profile should serialize");

        assert_eq!(profile["counters"][0]["name"], "process_rss");
        assert_eq!(
            profile["meta"]["markerSchema"][0]["name"],
            "GlueSQL tracing"
        );
        assert_marker_exists(&profile);
    }

    fn assert_marker_exists(profile: &Value) {
        assert_eq!(
            profile["threads"][0]["markers"]["data"]
                .as_array()
                .expect("marker data should be an array")
                .len(),
            1
        );
    }
}
