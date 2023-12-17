#[rustler::nif]
fn glue(a: i64, b: i64) -> i64 {
    a + b
}

rustler::init!("Elixir.GlueSQL.Native", [glue]);
