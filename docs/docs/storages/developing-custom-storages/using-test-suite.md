---
sidebar_position: 3
---

# Using the Test Suite

The GlueSQL Test Suite is a valuable tool for validating your custom storage implementation. By using the provided test sets, you can ensure that your storage implementation adheres to the required specifications and works as expected with GlueSQL.

To use the Test Suite, you will need to implement the `Tester` trait for your custom storage. A great reference for this process is the `MemoryStorage` implementation in the GlueSQL source code. Here's an example of how the `MemoryStorage` implementation looks like:

```rust
use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_memory_storage::MemoryStorage,
    test_suite::*,
};

struct MemoryTester {
    glue: Glue<MemoryStorage>,
}

#[async_trait]
impl Tester<MemoryStorage> for MemoryTester {
    async fn new(_: &str) -> Self {
        let storage = MemoryStorage::default();
        let glue = Glue::new(storage);

        MemoryTester { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<MemoryStorage> {
        &mut self.glue
    }
}
```

Once you have implemented the `Tester` trait, you can easily add the relevant test sets for the traits you have implemented in your custom storage. Here's how the `MemoryStorage` implementation adds the test sets:

```rust
generate_store_tests!(tokio::test, MemoryTester);

generate_alter_table_tests!(tokio::test, MemoryTester);

generate_metadata_table_tests!(tokio::test, MemoryTester);

generate_custom_function_tests!(tokio::test, MemoryTester);
```

The MemoryStorage example demonstrates the use of the four test sets from the Test Suite, indicating that it has implemented the `Store`, `StoreMut`, `AlterTable`, `CustomFunction`, `CustomFunctionMut`, and `Metadata` traits. However, you don't need to implement all Store traits for your custom storage. Instead, you can choose to implement only the traits that are relevant to your use case, and use the corresponding test sets from the Test Suite for validation.

The Test Suite provides test sets for the following traits:

- `generate_store_tests!` - Tests for `Store` and `StoreMut` implementations. (Note that `Store` and `StoreMut` are required for all other test sets.)
- `generate_alter_table_tests!` - Tests for the `AlterTable` trait implementation.
- `generate_custom_function_tests!` - Tests for the `CustomFunction` and `CustomFunctionMut` trait implementations.
- `generate_index_tests!` - Tests for the `Index` and `IndexMut` trait implementations.
- `generate_transaction_tests!` - Tests for the `Transaction` trait implementation.
- `generate_metadata_table_tests!` - Tests for the `Metadata` trait implementation.

Additionally, the Test Suite provides combined test sets for cases where you have implemented multiple optional traits:

- `generate_alter_table_index_tests!` - Tests for the `AlterTable`, `Index`, and `IndexMut` trait implementations.
- `generate_transaction_alter_table_tests!` - Tests for the `Transaction` and `AlterTable` trait implementations.
- `generate_transaction_index_tests!` - Tests for the `Transaction`, `Index`, and `IndexMut` trait implementations.
- `generate_metadata_index_tests!` - Tests for the `Metadata`, `Index`, and `IndexMut` trait implementations.

In summary, the GlueSQL Test Suite is an essential tool for validating your custom storage implementation. By using the provided test sets and the `MemoryStorage` implementation as an example, you can ensure your storage works correctly with GlueSQL and adheres to the necessary specifications.