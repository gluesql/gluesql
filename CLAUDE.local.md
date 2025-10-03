# GlueSQL Codebase Analysis - CLAUDE.local.md

## Project Overview

**GlueSQL** is a multi-model database engine written in Rust that provides SQL capabilities as a library. It's designed to be adaptable to various storage backends and supports both structured and unstructured data.

### Key Characteristics
- **Language**: Rust (Rust Edition 2024, Toolchain 1.88)
- **Architecture**: Modular database engine with pluggable storage backends
- **License**: Apache-2.0
- **Version**: 0.17.0

## Project Structure

### Core Components

- **`core/`** - The main database engine containing:
  - `ast/` - Abstract Syntax Tree definitions
  - `ast_builder/` - Query builder for programmatic query construction
  - `data/` - Data type definitions and value handling
  - `executor/` - Query execution engine
  - `plan/` - Query planning and optimization
  - `store/` - Storage interface traits
  - `translate/` - SQL parsing and translation

- **`cli/`** - Command-line interface for GlueSQL

- **`test-suite/`** - Comprehensive test suite covering all database features

- **`utils/`** - Utility functions and data structures

### Storage Backends (`storages/`)

GlueSQL supports multiple storage backends:
- **memory-storage** - In-memory storage (non-persistent)
- **shared-memory-storage** - Thread-safe in-memory storage
- **sled-storage** - Persistent storage using Sled embedded database
- **redb-storage** - Persistent storage using redb embedded database
- **json-storage** - JSON/JSONL file-based storage
- **csv-storage** - CSV file-based storage
- **parquet-storage** - Apache Parquet file storage
- **file-storage** - General filesystem-based storage
- **git-storage** - Git repository as storage backend
- **mongo-storage** - MongoDB as storage backend
- **redis-storage** - Redis as storage backend
- **web-storage** - Browser localStorage/sessionStorage
- **idb-storage** - Browser IndexedDB storage
- **composite-storage** - Combines multiple storage backends

### Language Bindings (`pkg/`)

- **`rust/`** - Native Rust API
- **`javascript/`** - JavaScript/WebAssembly bindings
- **`python/`** - Python bindings

## Build System

### Primary Commands
- `cargo test` - Run all tests
- `cargo clippy` - Run linter
- `cargo fmt` - Format code
- `cargo build` - Build the project
- `cargo build --release` - Build optimized release

### Workspace Structure
- Uses Cargo workspace with 22+ member crates
- All crates share version 0.17.0
- Test coverage is nearly 99%

## Development Guidelines

### Code Standards
- **Linting**: Uses `clippy` with `#![deny(clippy::str_to_string)]`
- **Formatting**: Uses `rustfmt`
- **Testing**: Comprehensive test-driven development approach

### Key Files to Know

#### Core Engine Files
- `core/src/lib.rs:19-30` - Main prelude exports
- `core/src/glue.rs` - Main database interface
- `core/src/executor/` - Query execution logic
- `core/src/plan/` - Query planning

#### Storage Interface
- `core/src/store/` - Storage trait definitions
- Custom storages must implement `Store` and `StoreMut` traits

#### Testing
- `test-suite/src/lib.rs` - Comprehensive test modules
- Test suite covers: aggregation, joins, data types, functions, etc.

## Architecture Patterns

### Storage Abstraction
- **Trait-based**: Storage backends implement standard traits
- **Pluggable**: Easy to add new storage types
- **Flexible**: Supports both SQL and schemaless data

### Query Processing Pipeline
1. **Parse** - SQL to AST conversion
2. **Plan** - Query optimization and planning  
3. **Execute** - Query execution against storage
4. **Result** - Payload return with results

### Data Model
- Supports both structured (with schema) and unstructured (schemaless) data
- Complex data types: MAP, LIST, custom types
- Can join structured and unstructured tables

## Testing Strategy

### Test Categories
- **Unit tests** - In individual modules
- **Integration tests** - In `test-suite/` crate
- **Storage tests** - Each storage has comprehensive tests
- **Language binding tests** - For JavaScript/Python APIs

### Running Tests
```bash
# Run all tests
cargo test

# Run specific storage tests
cargo test --package gluesql-memory-storage

# Run core tests only  
cargo test --package gluesql-core
```

## Common Development Tasks

### Adding New Features
1. Implement in `core/` modules
2. Add tests in `test-suite/`
3. Update relevant storage implementations
4. Run full test suite

### Creating Custom Storage
1. Implement `Store` and `StoreMut` traits
2. Add optional traits for advanced features
3. Use test suite for validation
4. Follow existing storage patterns

### Working with AST Builder
- Alternative to SQL strings
- Type-safe query construction
- Full feature parity with SQL

## Git Information
- **Current Branch**: add-typed-array
- **Main Branch**: main
- **Status**: Clean working directory
- Recent focus on agent documentation and test coverage

## Additional Resources
- **Documentation**: https://gluesql.org/docs
- **Repository**: https://github.com/gluesql/gluesql
- **Discord**: Community support available
- **Blog**: https://gluesql.org/blog for deep dives

---
*This analysis is based on codebase structure as of the current commit and may change as the project evolves.*