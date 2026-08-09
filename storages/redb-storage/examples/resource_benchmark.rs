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

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("gluesql=info")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(io::stderr)
        .init();

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
    Ok(())
}

fn record_memory_sample(parent: &Span, started_at: Instant) -> io::Result<()> {
    tracing::debug!(
        target: "gluesql",
        parent: parent,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        rss_bytes = current_rss_bytes()?,
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
