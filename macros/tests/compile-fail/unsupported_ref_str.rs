use gluesql::FromGlueRow;

#[derive(FromGlueRow)]
struct S<'a> {
    v: &'a str,
}

