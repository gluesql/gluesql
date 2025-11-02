import org.gradle.api.GradleException
import org.gradle.internal.os.OperatingSystem

/**
 * Platform-specific configuration for native library builds.
 *
 * @property rustTarget Rust target triple (e.g., "x86_64-unknown-linux-gnu")
 * @property jnaPrefix JNA platform prefix for resource directories (e.g., "linux-x86-64")
 * @property libExtension Native library file extension (e.g., "so", "dylib", "dll")
 */
data class PlatformConfig(
    val rustTarget: String,
    val jnaPrefix: String,
    val libExtension: String,
) {
    /** Library name without 'lib' prefix (e.g., "gluesql_kotlin.so") */
    val libNameWithoutPrefix: String
        get() = "gluesql_kotlin.$libExtension"

    /** Library name with 'lib' prefix (e.g., "libgluesql_kotlin.so") */
    val libNameWithPrefix: String
        get() = "libgluesql_kotlin.$libExtension"

    /**
     * Platform-appropriate library file name.
     * Windows doesn't use 'lib' prefix, while Unix-like systems do.
     */
    val libFileName: String
        get() = if (jnaPrefix.startsWith("win32")) libNameWithoutPrefix else libNameWithPrefix
}

/**
 * List of all supported target platforms for cross-compilation.
 * These are used for building release artifacts on CI.
 */
val platforms = listOf(
    PlatformConfig("x86_64-unknown-linux-gnu", "linux-x86-64", "so"),
    PlatformConfig("x86_64-apple-darwin", "darwin-x86-64", "dylib"),
    PlatformConfig("aarch64-apple-darwin", "darwin-aarch64", "dylib"),
    PlatformConfig("x86_64-pc-windows-msvc", "win32-x86-64", "dll"),
)

/**
 * Returns the native library name for the current platform.
 * @return Library name with or without 'lib' prefix depending on OS
 */
fun currentPlatformLibName(): String =
    when {
        OperatingSystem.current().isWindows -> "gluesql_kotlin"
        else -> "libgluesql_kotlin"
    }

/**
 * Returns the file extension for native libraries on the current platform.
 * @return "so" for Linux, "dylib" for macOS, "dll" for Windows
 * @throws GradleException if the platform is not supported
 */
fun currentPlatformLibExtension(): String =
    when {
        OperatingSystem.current().isLinux -> "so"
        OperatingSystem.current().isMacOsX -> "dylib"
        OperatingSystem.current().isWindows -> "dll"
        else -> throw GradleException("Unsupported operating system: ${OperatingSystem.current()}")
    }
