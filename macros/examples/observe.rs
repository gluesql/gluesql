use {gluesql_macros::observe, tracing_subscriber::fmt::format::FmtSpan};

#[observe(
    name = "gluesql.example.collect",
    fields(input_rows = input.len()),
    after_let(rows, record(buffered_rows = rows.len())),
    count_loop(binding = row, field = scanned_rows),
    on_ok(rows, record(returned_rows = rows.len())),
    err(Debug)
)]
fn collect(input: &[i32]) -> Result<Vec<i32>, &'static str> {
    let rows = input.to_vec();
    for row in &rows {
        if *row < 0 {
            return Err("negative row");
        }
    }
    Ok(rows)
}

#[observe(
    name = "gluesql.example.collect_keys",
    start = before_let(keys),
    end = after_let(num_keys),
    record(buffered_rows = num_keys)
)]
fn count_keys(input: &[i32]) -> usize {
    let keys = input.to_vec();
    let num_keys = keys.len();
    // This work is outside the collection span.
    assert_eq!(num_keys, input.len());
    num_keys
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_span_events(FmtSpan::CLOSE)
        .with_ansi(false)
        .without_time()
        .with_writer(std::io::stderr)
        .init();

    assert_eq!(collect(&[1, 2, 3]), Ok(vec![1, 2, 3]));
    assert_eq!(collect(&[1, -2, 3]), Err("negative row"));
    assert_eq!(count_keys(&[1, 2, 3]), 3);
    println!("All observation example checks passed.");
}
