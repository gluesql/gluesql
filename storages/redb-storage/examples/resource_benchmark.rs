use {
    gluesql_core::prelude::Glue,
    gluesql_redb_storage::RedbStorage,
    std::{env, error::Error, fs, io, path::Path},
    tracing::field,
    tracing_subscriber::fmt::format::FmtSpan,
};

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

    let sql = fs::read_to_string(&sql_path)?;
    let storage = RedbStorage::new(&database_path)?;
    let mut glue = Glue::new(storage);
    for statement in sql.split(';').filter(|sql| !sql.trim().is_empty()) {
        glue.execute(statement)?;
    }
    drop(glue);

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
    use super::peak_rss_bytes;

    #[test]
    fn peak_rss_is_reported_in_bytes() {
        assert!(peak_rss_bytes().expect("peak RSS should be available") > 0);
    }
}
