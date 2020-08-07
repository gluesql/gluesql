use gluesql::generate_tests;
use sled_storage::SledTester;

generate_tests!(SledTester);
