use gluesql_macros::FromGlueRow;

#[derive(FromGlueRow)]
struct S<'a> {
    v: &'a str,
}
