use {
    gluesql_core::prelude::Glue,
    gluesql_redb_storage::RedbStorage,
    std::{
        env,
        error::Error,
        fs, io,
        path::Path,
        sync::mpsc::{self, RecvTimeoutError, Sender},
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
    tracing::{Level, Span, field},
    tracing_subscriber::fmt::format::FmtSpan,
};

const DEFAULT_MEMORY_SAMPLE_MS: u64 = 10;

struct MemorySampler {
    stop: Sender<()>,
    handle: JoinHandle<io::Result<()>>,
}

impl MemorySampler {
    fn start(parent: Span, interval: Duration) -> Self {
        let (stop, receiver) = mpsc::channel();
        let started_at = Instant::now();
        let handle = thread::spawn(move || {
            loop {
                record_memory_sample(&parent, started_at)?;

                match receiver.recv_timeout(interval) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                        record_memory_sample(&parent, started_at)?;
                        return Ok(());
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        });

        Self { stop, handle }
    }

    fn stop(self) -> io::Result<()> {
        let _ = self.stop.send(());
        self.handle
            .join()
            .map_err(|_| io::Error::other("memory sampler thread panicked"))?
    }
}

#[cfg(feature = "perfetto")]
fn perfetto_trace_config() -> tracing_perfetto_sdk_schema::TraceConfig {
    use tracing_perfetto_sdk_schema::{DataSourceConfig, trace_config};

    tracing_perfetto_sdk_schema::TraceConfig {
        buffers: vec![trace_config::BufferConfig {
            size_kb: Some(1024),
            ..Default::default()
        }],
        data_sources: vec![trace_config::DataSource {
            config: Some(DataSourceConfig {
                name: Some("rust_tracing".into()),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[cfg(feature = "perfetto")]
fn init_tracing()
-> Result<tracing_perfetto_sdk_layer::NativeLayer<std::sync::Arc<fs::File>>, Box<dyn Error>> {
    use {
        tracing_perfetto_sdk_layer::NativeLayer,
        tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt},
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("gluesql=info"));
    let fmt_layer = fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(io::stderr);
    let path =
        env::var_os("GLUESQL_PERFETTO_PATH").unwrap_or_else(|| "gluesql-benchmark.pftrace".into());
    let perfetto_layer = NativeLayer::from_config(
        perfetto_trace_config(),
        std::sync::Arc::new(fs::File::create(path)?),
    )
    .build()?;
    let guard = perfetto_layer.clone();

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(perfetto_layer)
        .try_init()?;

    Ok(guard)
}

#[cfg(not(feature = "perfetto"))]
fn init_tracing() -> Result<(), Box<dyn Error>> {
    use tracing_subscriber::{EnvFilter, util::SubscriberInitExt};

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("gluesql=info")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(io::stderr)
        .try_init()?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "perfetto")]
    let perfetto_guard = init_tracing()?;
    #[cfg(not(feature = "perfetto"))]
    init_tracing()?;

    let mut args = env::args_os().skip(1);
    let database_path = args.next().ok_or("missing DATABASE_PATH")?;
    let sql_path = args.next().ok_or("missing SQL_PATH")?;
    if args.next().is_some() {
        return Err("usage: resource_benchmark DATABASE_PATH SQL_PATH".into());
    }

    let benchmark_name = Path::new(&sql_path)
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("redb");
    let span = tracing::info_span!(
        target: "gluesql",
        "gluesql.benchmark.run",
        benchmark.name = benchmark_name,
        benchmark.storage = "redb",
        process.memory.peak_bytes = field::Empty,
        gluesql.database.size_bytes = field::Empty,
        process.executable.size_bytes = field::Empty,
    );
    let entered = span.enter();

    let memory_sampler = if tracing::enabled!(target: "gluesql", Level::DEBUG) {
        let memory_sample_ms = env::var("GLUESQL_MEMORY_SAMPLE_MS")
            .map_or(Ok(DEFAULT_MEMORY_SAMPLE_MS), |value| value.parse::<u64>())?;
        if memory_sample_ms == 0 {
            return Err("GLUESQL_MEMORY_SAMPLE_MS must be greater than zero".into());
        }

        Some(MemorySampler::start(
            span.clone(),
            Duration::from_millis(memory_sample_ms),
        ))
    } else {
        None
    };

    let workload = (|| -> Result<(), Box<dyn Error>> {
        let sql = fs::read_to_string(&sql_path)?;
        let storage = RedbStorage::new(&database_path)?;
        let mut glue = Glue::new(storage);
        for statement in sql.split(';').filter(|sql| !sql.trim().is_empty()) {
            glue.execute(statement)?;
        }

        Ok(())
    })();
    if let Some(memory_sampler) = memory_sampler {
        memory_sampler.stop()?;
    }
    workload?;

    span.record("process.memory.peak_bytes", peak_rss_bytes()?);
    span.record(
        "gluesql.database.size_bytes",
        fs::metadata(database_path)?.len(),
    );
    span.record(
        "process.executable.size_bytes",
        fs::metadata(env::current_exe()?)?.len(),
    );

    drop(entered);
    drop(span);
    #[cfg(feature = "perfetto")]
    perfetto_guard.stop()?;
    Ok(())
}

fn record_memory_sample(parent: &Span, started_at: Instant) -> io::Result<()> {
    let elapsed_ms = started_at.elapsed().as_millis() as u64;
    let rss_bytes = current_rss_bytes()?;

    #[cfg(feature = "perfetto")]
    tracing::debug!(
        target: "gluesql",
        parent: parent,
        elapsed_ms,
        rss_bytes,
        counter.process_rss.bytes = rss_bytes,
        "gluesql.benchmark.memory_sample"
    );

    #[cfg(not(feature = "perfetto"))]
    tracing::debug!(
        target: "gluesql",
        parent: parent,
        elapsed_ms,
        rss_bytes,
        "gluesql.benchmark.memory_sample"
    );

    Ok(())
}

#[cfg(target_vendor = "apple")]
#[allow(deprecated)]
fn current_rss_bytes() -> io::Result<u64> {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::uninit();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;

    // SAFETY: task_info writes at most `count` integers to the correctly sized output buffer.
    let result = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &raw mut count,
        )
    };
    if result != libc::KERN_SUCCESS {
        return Err(io::Error::other(format!(
            "failed to read current RSS: Mach error {result}"
        )));
    }

    // SAFETY: the successful task_info call above initialized `info`.
    Ok(unsafe { info.assume_init() }.resident_size)
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> io::Result<u64> {
    let statm = fs::read_to_string("/proc/self/statm")?;
    let resident_pages = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing resident page count"))?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    // SAFETY: sysconf reads the process configuration and does not dereference pointers.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(io::Error::last_os_error());
    }

    resident_pages
        .checked_mul(page_size as u64)
        .ok_or_else(|| io::Error::other("current RSS overflowed u64"))
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
fn current_rss_bytes() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "current RSS measurement is supported only on macOS and Linux",
    ))
}

#[cfg(unix)]
fn peak_rss_bytes() -> io::Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();

    // SAFETY: getrusage initializes `usage` when it returns zero.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: the successful getrusage call above initialized `usage`.
    let peak_rss = unsafe { usage.assume_init() }.ru_maxrss as u64;

    #[cfg(target_vendor = "apple")]
    return Ok(peak_rss);

    #[cfg(not(target_vendor = "apple"))]
    Ok(peak_rss * 1024)
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "peak RSS measurement is supported only on Unix",
    ))
}

#[cfg(test)]
mod tests {
    use super::{current_rss_bytes, peak_rss_bytes};

    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn current_rss_is_reported_in_bytes() {
        assert!(current_rss_bytes().expect("current RSS should be available") > 0);
    }

    #[cfg(unix)]
    #[test]
    fn peak_rss_is_reported_in_bytes() {
        assert!(peak_rss_bytes().expect("peak RSS should be available") > 0);
    }
}
