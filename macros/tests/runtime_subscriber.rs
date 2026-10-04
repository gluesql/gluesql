use {gluesql_core::prelude::Glue, gluesql_memory_storage::MemoryStorage};

#[test]
fn application_registers_subscriber_after_unobserved_queries() {
    assert!(!tracing::dispatcher::has_been_set());
    let mut glue = Glue::new(MemoryStorage::default());
    glue.execute("SELECT 1").unwrap();
    assert!(!tracing::dispatcher::has_been_set());

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(std::io::sink)
        .try_init()
        .unwrap();
    assert!(tracing::dispatcher::has_been_set());
    glue.execute("SELECT 2").unwrap();
}
