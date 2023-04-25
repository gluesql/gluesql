---
sidebar_position: 1
---

# Introduction

With GlueSQL, you can adapt SQL and the AST Builder to a wide variety of environments. This includes file systems, key-value databases, complex NoSQL databases, and even remote APIs. As long as a system supports reading, it can support SELECT queries. If it supports both reading and writing, it can support most SQL operations, including UPDATE and DELETE.

To implement GlueSQL, you only need to know two things:
1. Understanding Store traits
2. Using the Test Suite

These topics are covered in more detail in their respective pages, but here we will provide a brief overview.

## Understanding Store Traits

GlueSQL is available in both Rust and JavaScript environments, with plans to expand its support to other languages. Since the GlueSQL project itself is written in Rust, using Rust is essential for developing custom storages. To create a custom storage, you need to implement the Store traits provided by GlueSQL. There are currently 9 traits:

* `Store` - A trait for read operations to support SELECT queries.
* `StoreMut` - A trait for modifying data, such as INSERT, UPDATE, and DELETE.
* `AlterTable` - A trait for supporting schema changes.
* `Transaction` - A trait for supporting transactions.
* `CustomFunction` - A trait for supporting user-level custom functions.
* `CustomFunctionMut` - A trait for creating or deleting user-level custom functions.
* `Index` - A trait for supporting non-clustered indexes. This trait allows you to process pre-registered indexes.
* `IndexMut` - A trait for creating or deleting non-clustered indexes.
* `Metadata` - A trait for querying metadata.

To develop a custom storage, you can implement these 9 traits. Although this may seem like a lot of work, don't worry. All traits except for `Store` and `StoreMut` are optional. In other words, you don't have to implement them. If you don't want to support the `AlterTable` trait, simply don't implement it. It's that simple.

Furthermore, if you only want to support SELECT queries, you don't need to implement `StoreMut`. By implementing only the `Store` trait, you can create a custom storage that supports SQL SELECT queries.

## Using the Test Suite

The minimum requirement for developing a custom storage is implementing the Store traits. However, you may want to verify that your implementation is correct. That's where the Test Suite comes in.

GlueSQL provides a test case library to make it easy to validate custom storage development. Developers can implement the desired Store traits and use the Test Suite to verify that their implementation is correct. The line coverage of GlueSQL's core project is almost 99%, which means that passing the Test Suite alone can complete most of the feature verification. All you need to do is ensure your custom storage passes the Test Suite and write additional tests for any specialized features specific to your storage.