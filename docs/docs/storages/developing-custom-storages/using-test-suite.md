---
sidebar_position: 3
---

# Using the Test Suite

The Test Suite checks shared SQL behavior. Implement `Tester` to run it against your storage. For example, MemoryStorage uses:

```rust
use {gluesql_core::prelude::Glue, gluesql_memory_storage::MemoryStorage, test_suite::*};

struct MemoryTester {
    glue: Glue<MemoryStorage>,
}

impl Tester<MemoryStorage> for MemoryTester {
    fn new(_: &str) -> Self {
        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        MemoryTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MemoryStorage> {
        &mut self.glue
    }
}
```

Once you have implemented the `Tester` trait, add the test sets for the features your storage supports. Here's how the `MemoryStorage` implementation adds the test sets:

```rust
generate_store_tests!(test, MemoryTester);

generate_alter_table_tests!(test, MemoryTester);

generate_metadata_table_tests!(test, MemoryTester);

generate_custom_function_tests!(test, MemoryTester);
```

This registers tests for reads, writes, schema changes, custom functions, and metadata. Choose the test sets for the features your storage supports.

The Test Suite provides test sets for the following traits:

- `generate_store_tests!` - Tests for `Store` and `StoreMut` implementations.
- `generate_alter_table_tests!` - Tests for the `AlterTable` trait implementation.
- `generate_custom_function_tests!` - Tests for the `CustomFunction` and `CustomFunctionMut` trait implementations.
- `generate_index_tests!` - Tests for the `Index` and `IndexMut` trait implementations.
- `generate_transaction_tests!` - Tests for the `Transaction` trait implementation.
- `generate_metadata_table_tests!` - Tests for the `Metadata` trait implementation.

Additionally, the Test Suite provides combined test sets for storage features used together:

- `generate_alter_table_index_tests!` - Tests for the `AlterTable`, `Index`, and `IndexMut` trait implementations.
- `generate_transaction_alter_table_tests!` - Tests for the `Transaction` and `AlterTable` trait implementations.
- `generate_transaction_index_tests!` - Tests for the `Transaction`, `Index`, and `IndexMut` trait implementations.
- `generate_metadata_index_tests!` - Tests for the `Metadata`, `Index`, and `IndexMut` trait implementations.

Keep storage-specific checks, such as reopening database files, transaction isolation, and backend failures, in your storage crate's integration tests. See the [testing policy](../../development/review-policy/testing.md#storage-integration-tests) for the boundary between shared SQL cases and storage-specific tests.
