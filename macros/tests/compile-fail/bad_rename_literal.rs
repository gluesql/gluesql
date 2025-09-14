use gluesql::FromGlueRow;

#[derive(FromGlueRow)]
struct S {
    #[glue(rename = 123)]
    v: i32,
}

