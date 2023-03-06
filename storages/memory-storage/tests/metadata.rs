// use gluesql_memory_storage::MemoryStorage;

// #[test]
// fn test() {
//     use gluesql_core::{
//         prelude::{Glue, Payload},
//         result::Error,
//     };

//     macro_rules! exec {
//         ($glue: ident $sql: literal) => {
//             $glue.execute($sql).unwrap();
//         };
//     }
//     macro_rules! test {
//         ($glue: ident $sql: literal, $result: expr) => {
//             assert_eq!($glue.execute($sql), $result);
//         };
//     }
//     let storage = MemoryStorage::default();
//     let mut glue = Glue::new(storage);

//     // exec!(glue "CREATE TABLE MetaTest (id INTEGER);");
//     // scan_meta()
// }
