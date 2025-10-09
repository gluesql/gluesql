# Development Guide

## Prerequisites

- Rust 1.70+
- Java 11+
- Kotlin 1.9+
- Gradle 8.0+

## Build

```bash
# Build Rust library and generate bindings
./gradlew build

# Run tests
./gradlew test
```

## Project Structure

```
pkg/java/
├── src/
│   ├── lib.rs              # Main Rust library
│   ├── uniffi_types.rs     # UniFFI type definitions
│   ├── executor.rs         # Query executor
│   ├── storage.rs          # Storage backend
│   └── error.rs            # Error handling
├── src/main/kotlin/        # Kotlin source files
├── src/test/               # Test files
├── build.gradle.kts        # Gradle build script
└── Cargo.toml              # Rust dependencies
```

## Development Workflow

### Making Changes to Rust Code

```bash
# Clean and rebuild
./gradlew clean build
```

### Regenerating Bindings

```bash
# Generate Kotlin bindings from Rust
./gradlew generateBindings
```

### Running Tests

```bash
# Run all tests
./gradlew test

# Run specific test
./gradlew test --tests "SqlValueTest"
```

## Code Style & Formatting

We use `Spotless` for code formatting with:
- **Kotlin**: ktlint default conventions
- **Java**: Google Java Format default style
- Automatic removal of unused imports and trailing whitespace
- Files end with newline

```bash
# Check code formatting
./gradlew spotlessCheck

# Auto-format all code
./gradlew spotlessApply
```

### Pre-commit Formatting

Before committing changes, always run:

```bash
# Format all code and run tests
./gradlew spotlessApply test
```

## Common Issues

### Library Not Found Error

If you see `UnsatisfiedLinkError`:

```bash
# Ensure Rust library is built
cargo build

# Check library exists
ls -la ../../target/debug/libgluesql_java.*
```

### Binding Sync Issues

If bindings are outdated:

```bash
# Clean and regenerate
./gradlew clean generateBindings
```

## Adding New Features

1. Implement in Rust (`src/`)
2. Update UniFFI types if needed (`uniffi_types.rs`)
3. Regenerate bindings (`./gradlew generateBindings`)
4. Add tests
5. Update documentation