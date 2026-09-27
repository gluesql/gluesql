use gluesql_macros::ToGlueRow;

#[derive(ToGlueRow)]
enum E {
    A,
}
