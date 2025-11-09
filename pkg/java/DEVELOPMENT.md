# Development Guide

This guide is for developers who want to contribute to or modify the GlueSQL Java/Kotlin bindings.

## Prerequisites

- **Rust** 1.70+ ([Install Rust](https://rustup.rs/))
- **Java** 11+ (JDK)
- **Kotlin** 1.9+ (bundled with Gradle)
- **Gradle** 8.0+ (use included `./gradlew`)

## Quick Start

```bash
# Clone the repository
git clone https://github.com/gluesql/gluesql.git
cd gluesql/pkg/java

# Build and test
./gradlew test
```

## Project Structure

```
pkg/java/
├── src/                    # Rust source files
│   ├── lib.rs             # Main library entry point
│   ├── uniffi_types.rs    # UniFFI type definitions
│   ├── executor.rs        # Query executor implementation
│   ├── storage.rs         # Storage backend wrapper
│   └── error.rs           # Error handling
├── src/main/kotlin/       # Kotlin/Java source files
│   └── org/gluesql/
│       ├── client/        # High-level client APIs
│       └── storage/       # Storage factory
├── src/test/              # Test files
├── build.gradle.kts       # Gradle build configuration
├── Cargo.toml             # Rust dependencies
└── uniffi-bindgen.rs      # UniFFI code generator
```

## Development Workflow

### 1. Making Changes to Rust Code

When you modify Rust code:

```bash
# Clean build (recommended after Rust changes)
./gradlew clean build

# Or just rebuild and test
./gradlew test
```

**How it works:**
1. `buildRustLib` task compiles Rust → `../../target/debug/libgluesql_java.*`
2. `generateBindings` task runs UniFFI → generates Kotlin bindings
3. Kotlin code is compiled with generated bindings

### 2. Making Changes to Kotlin/Java Code

```bash
# Kotlin/Java changes don't need Rust rebuild
./gradlew compileKotlin test
```

### 3. Regenerating Bindings (Manual)

If you modify UniFFI interface definitions:

```bash
./gradlew generateBindings
```

This runs:
```bash
cargo run --bin uniffi-bindgen generate \
    --language kotlin \
    --library ../../target/debug/libgluesql_java.dylib
```

### 4. Running Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "org.gluesql.NativeLibraryLoadingTest"

# Run with verbose output
./gradlew test --info
```

## Code Style & Formatting

We use **Spotless** for automatic code formatting:

- **Kotlin**: ktlint conventions
- **Java**: Google Java Format

### Commands

```bash
# Check formatting (runs in CI)
./gradlew spotlessCheck

# Auto-format all code
./gradlew spotlessApply
```

### Pre-commit Checklist

Before committing:

```bash
# 1. Format code
./gradlew spotlessApply

# 2. Run tests
./gradlew test

# 3. Or combine both
./gradlew spotlessApply test
```

## Build Tasks Reference

| Task | Description |
|------|-------------|
| `./gradlew build` | Full build (Rust + Kotlin + tests) |
| `./gradlew test` | Run all tests |
| `./gradlew clean` | Clean build artifacts |
| `./gradlew buildRustLib` | Build Rust library (debug mode) |
| `./gradlew generateBindings` | Generate UniFFI Kotlin bindings |
| `./gradlew copyNativeLibs` | Copy release libraries to resources (for distribution) |
| `./gradlew cleanNativeLibs` | Remove native libraries from resources |
| `./gradlew spotlessCheck` | Check code formatting |
| `./gradlew spotlessApply` | Auto-format code |

## Distribution Builds

### Local Testing

Development builds use **debug mode** Rust libraries:

```bash
./gradlew test
# Uses: ../../target/debug/libgluesql_java.*
```

### Production JAR

For distribution, we build a **Fat JAR** with native libraries for all platforms.

**This requires building on multiple platforms** (handled by CI):

```yaml
# .github/workflows/publish-java.yml
1. Build native libraries on each platform (Linux, macOS, Windows)
2. Collect all binaries as artifacts
3. Copy to src/main/resources/natives/{platform}/
4. Build final JAR with all libraries included
```

**To test locally** (if you have cross-compilation set up):

```bash
# Build for current platform only
cargo build --release --target aarch64-apple-darwin

# Copy to resources
./gradlew copyNativeLibs

# Build JAR
./gradlew build

# Check JAR contents
jar tf build/libs/gluesql-0.1.0.jar | grep natives
```

## Common Issues & Solutions

### Issue: `UnsatisfiedLinkError: Unable to load library 'gluesql_java'`

**Cause:** Rust library not built or not in expected location

**Solution:**
```bash
# Rebuild Rust library
cargo build

# Verify it exists
ls -la ../../target/debug/libgluesql_java.*

# Clean and rebuild
./gradlew clean build
```

### Issue: Bindings out of sync with Rust code

**Cause:** Rust interface changed but Kotlin bindings not regenerated

**Solution:**
```bash
./gradlew clean generateBindings
```

### Issue: `Gradle task 'generateBindings' failed`

**Cause:** UniFFI can't find the library or library is corrupted

**Solution:**
```bash
# Clean everything
./gradlew clean
rm -rf ../../target/debug/libgluesql_java.*

# Rebuild from scratch
./gradlew build
```

### Issue: Tests pass locally but fail in CI

**Cause:** Platform-specific issues or missing dependencies

**Solution:**
- Check GitHub Actions logs
- Ensure all platforms are tested
- Verify native library paths in CI config

## Adding New Features

### 1. Add Rust Function

Edit `src/lib.rs` or relevant module:

```rust
#[uniffi::export]
impl Glue {
    pub async fn new_feature(&self, param: String) -> Result<String, GlueSQLError> {
        // Implementation
        Ok(format!("Result: {}", param))
    }
}
```

### 2. Regenerate Bindings

```bash
./gradlew generateBindings
```

This creates Kotlin bindings automatically:
```kotlin
// Auto-generated in build/generated/source/uniffi/kotlin/
class Glue {
    suspend fun newFeature(param: String): String { ... }
}
```

### 3. Add High-Level API (Optional)

Create user-friendly wrapper in `src/main/kotlin/`:

```kotlin
// src/main/kotlin/org/gluesql/client/kotlin/GlueSQLClient.kt
suspend fun GlueSQLClient.newFeature(param: String): String {
    return glue.newFeature(param)
}
```

### 4. Add Tests

```kotlin
// src/test/kotlin/org/gluesql/NewFeatureTest.kt
@Test
fun `test new feature`() = runBlocking {
    val client = GlueSQLClient(StorageFactory.memory())
    val result = client.newFeature("test")
    assertEquals("Result: test", result)
}
```

### 5. Run Tests

```bash
./gradlew test --tests "NewFeatureTest"
```

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│           JVM (Java/Kotlin)                     │
│                                                 │
│  ┌──────────────┐         ┌─────────────┐     │
│  │ User's Code  │────────▶│ GlueSQLClient│     │
│  └──────────────┘         └─────────────┘     │
│                                  │             │
│                           ┌──────▼─────────┐  │
│                           │ UniFFI Generated│  │
│                           │    Bindings     │  │
│                           └────────────────┘  │
│                                  │             │
└──────────────────────────────────┼─────────────┘
                                   │ JNA (FFI)
┌──────────────────────────────────▼─────────────┐
│           Rust Native Library                   │
│                                                 │
│  ┌─────────────┐      ┌──────────────────┐    │
│  │    Glue     │─────▶│ QueryExecutor    │    │
│  └─────────────┘      └──────────────────┘    │
│         │                      │               │
│         │             ┌────────▼──────────┐   │
│         │             │  GlueSQL Core     │   │
│         │             └───────────────────┘   │
│         │                                      │
│  ┌──────▼────────┐                            │
│  │ StorageBackend│                            │
│  └───────────────┘                            │
└─────────────────────────────────────────────────┘
```

## Debugging Tips

### Enable JNA Debug Logging

```kotlin
System.setProperty("jna.debug_load", "true")
System.setProperty("jna.debug_load.jna", "true")
```

### Check Native Library Loading

```bash
# Run test with detailed output
./gradlew test --tests "NativeLibraryLoadingTest" --info
```

### Inspect Generated Bindings

```bash
# View generated Kotlin code
cat build/generated/source/uniffi/kotlin/org/gluesql/uniffi/gluesql_java.kt
```

### Verify JAR Contents

```bash
# List all files in JAR
jar tf build/libs/gluesql-0.1.0.jar

# Check for native libraries
jar tf build/libs/gluesql-0.1.0.jar | grep natives
```

## Release Process

See `.github/workflows/publish-java.yml` for automated release process:

1. Push tag or trigger workflow manually
2. CI builds native libraries on all platforms (parallel)
3. Artifacts collected and copied to `src/main/resources/natives/`
4. Fat JAR built with all native libraries
5. Published to Maven Central (if configured)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `./gradlew spotlessApply test`
5. Commit and push
6. Create a Pull Request

## Resources

- [UniFFI User Guide](https://mozilla.github.io/uniffi-rs/)
- [JNA Documentation](https://github.com/java-native-access/jna)
- [GlueSQL Documentation](https://gluesql.org/docs)
- [Rust Book](https://doc.rust-lang.org/book/)
- [Kotlin Coroutines](https://kotlinlang.org/docs/coroutines-overview.html)
