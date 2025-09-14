use gluesql::FromGlueRow;

#[derive(FromGlueRow)]
struct S {
    #[glue]
    v: i32,
}

