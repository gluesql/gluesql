---
sidebar_position: 1
---

# Rust aaaa

To install and use GlueSQL in your Rust project, you'll first need to add it as a dependency from crates.io. You can do this by adding the following lines to your `Cargo.toml` file:

```toml
[dependencies]
gluesql = "0.14"
```

By default, all available storage features are included with GlueSQL. Here's a list of the available features:

- `sled-storage` - Storage based on the persistent key-value database called sled
- `memory-storage` - Simple in-memory storage
- `shared-memory-storage` - A wrapper around memory-storage for easy use in multi-threaded environments
- `json-storage` - Storage that allows you to analyze and modify JSON or JSONL files using SQL
- `composite-storage` - A storage feature that enables joining and processing data from multiple storage types simultaneously
- `web-storage` - Storage supporting localStorage and sessionStorage, available only in web assembly builds
- `idb-storage` - IndexedDB-based storage, available only in web assembly builds

If you don't need all the default storage features, you can disable them and select only the ones you require. To do this, update your `Cargo.toml` file with the following lines:

```toml
[dependencies.gluesql]
version = "0.14"
default-features = false
features = ["memory-storage", "json-storage"]
```

This configuration will disable the default storage features and only include the `memory-storage` and `json-storage` features in your project.