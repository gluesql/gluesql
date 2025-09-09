use gluesql::FromGlueRow;

#[derive(FromGlueRow)]
enum E {
    A,
}

