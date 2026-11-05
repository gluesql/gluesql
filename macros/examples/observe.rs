use {gluesql_macros::observe, tracing_subscriber::fmt::format::FmtSpan};

#[observe]
fn collect(input: &[i32]) -> Result<Vec<i32>, &'static str> {
    let rows = copy_rows(input);
    validate_rows(&rows)?;
    Ok(rows)
}

#[observe]
fn copy_rows(input: &[i32]) -> Vec<i32> {
    input.to_vec()
}

#[observe]
fn validate_rows(rows: &[i32]) -> Result<(), &'static str> {
    if rows.iter().any(|row| *row < 0) {
        return Err("negative row");
    }
    Ok(())
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_span_events(FmtSpan::CLOSE)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .init();

    assert_eq!(collect(&[1, 2, 3]), Ok(vec![1, 2, 3]));
    assert_eq!(collect(&[1, -2, 3]), Err("negative row"));
    println!("All observation example checks passed.");
}
