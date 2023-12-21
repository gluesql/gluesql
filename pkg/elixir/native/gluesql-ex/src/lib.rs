#[rustler::nif]
fn glue(a: i64) -> i64 {
    a
}

rustler::init!("Elixir.GlueSQL.Native", [glue]);
