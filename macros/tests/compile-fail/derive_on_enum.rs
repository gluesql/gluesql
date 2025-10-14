use gluesql_macros::FromGlueRow;

#[derive(FromGlueRow)]
enum E {
    A,
}
