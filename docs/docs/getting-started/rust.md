---
sidebar_position: 1
---

# Rust

To install and use GlueSQL in your Rust project, you'll first need to add it as a dependency from crates.io. You can do this by adding the following lines to your `Cargo.toml` file:

```toml
[dependencies]
gluesql = "0.19.0"
```

By default, GlueSQL enables all bundled storage features. Here's a list of the available storage features:

- `gluesql_memory_storage` - Simple in-memory storage
- `gluesql-shared-memory-storage` - A wrapper around memory-storage for easy use in multi-threaded environments
- `gluesql_sled_storage` - Storage based on the persistent key-value database called sled
- `gluesql-redb-storage` - Storage using the redb embedded database
- `gluesql-json-storage` - Storage that allows you to analyze and modify JSON or JSONL files using SQL
- `gluesql-parquet-storage` - Storage for reading and writing Parquet files
- `gluesql-csv-storage` - Storage for reading and writing CSV files
- `gluesql-composite-storage` - A storage feature that enables joining and processing data from multiple storage types simultaneously
- `gluesql-mongo-storage` - Storage backed by MongoDB
- `gluesql-redis-storage` - Storage backed by Redis
- `gluesql-file-storage` - File-based persistent storage using schema files and row files
- `gluesql-git-storage` - Git-backed storage that commits schema and data changes

If you don't need all the default storage features, you can disable them and select only the ones you require. To do this, update your `Cargo.toml` file with the following lines:

```toml
[dependencies.gluesql]
version = "0.19.0"
default-features = false
features = ["gluesql_memory_storage", "gluesql-json-storage"]
```

This configuration will disable the default storage features and only include the `gluesql_memory_storage` and `gluesql-json-storage` features in your project.
