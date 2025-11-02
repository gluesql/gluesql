import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.file.FileTree
import org.gradle.internal.os.OperatingSystem
import org.gradle.kotlin.dsl.register
import java.io.File

data class PlatformConfig(
    val rustTarget: String,
    val jnaPrefix: String,
    val libExtension: String,
) {
    val libNameWithoutPrefix: String
        get() = "gluesql_kotlin.$libExtension"

    val libNameWithPrefix: String
        get() = "libgluesql_kotlin.$libExtension"

    // Windows doesn't use 'lib' prefix
    val libFileName: String
        get() = if (jnaPrefix.startsWith("win32")) libNameWithoutPrefix else libNameWithPrefix
}

val platforms =
    listOf(
        PlatformConfig("x86_64-unknown-linux-gnu", "linux-x86-64", "so"),
        PlatformConfig("x86_64-apple-darwin", "darwin-x86-64", "dylib"),
        PlatformConfig("aarch64-apple-darwin", "darwin-aarch64", "dylib"),
        PlatformConfig("x86_64-pc-windows-msvc", "win32-x86-64", "dll"),
    )

fun currentPlatformLibName(): String =
    when {
        OperatingSystem.current().isWindows -> "gluesql_kotlin"
        else -> "libgluesql_kotlin"
    }

fun currentPlatformLibExtension(): String =
    when {
        OperatingSystem.current().isLinux -> "so"
        OperatingSystem.current().isMacOsX -> "dylib"
        OperatingSystem.current().isWindows -> "dll"
        else -> throw GradleException("Unsupported operating system: ${OperatingSystem.current()}")
    }

fun Project.registerNativeBuildTasks() {
    val workspaceRoot = projectDir.parentFile.parentFile

    // Build Rust library for local platform (for development and testing)
    val buildRustLib =
        tasks.register("buildRustLib") {
            group = "build"
            description = "Build the Rust library for local platform (debug mode)"

            val debugLibPath = workspaceRoot.resolve("target/debug/${currentPlatformLibName()}.${currentPlatformLibExtension()}")
            outputs.file(debugLibPath)

            // Only rebuild if source files changed
            inputs.files(fileTree(projectDir.resolve("src")).include("**/*.rs"))
            inputs.file(projectDir.resolve("Cargo.toml"))

            doLast {
                exec {
                    workingDir = projectDir
                    commandLine("cargo", "build")
                }
            }
        }

    // Generate UniFFI bindings from Rust library
    val generateBindings =
        tasks.register("generateBindings") {
            group = "build"
            description = "Generate UniFFI Kotlin bindings from Rust library"
            dependsOn(buildRustLib)

            val generatedDir = layout.buildDirectory.dir("generated/source/uniffi/kotlin").get().asFile
            val debugLibPath = workspaceRoot.resolve("target/debug/${currentPlatformLibName()}.${currentPlatformLibExtension()}")

            inputs.file(debugLibPath)
            inputs.files(fileTree(projectDir.resolve("src")).include("**/*.rs", "**/*.udl"))
            outputs.dir(generatedDir)

            doLast {
                generatedDir.mkdirs()

                exec {
                    workingDir = projectDir
                    commandLine(
                        "cargo",
                        "run",
                        "--bin",
                        "uniffi-bindgen",
                        "generate",
                        "--language",
                        "kotlin",
                        "--out-dir",
                        generatedDir.absolutePath,
                        "--library",
                        debugLibPath.absolutePath,
                        "--no-format",
                    )
                }
            }
        }

    // Copy native libraries from CI artifacts or local builds to resources
    tasks.register("copyNativeLibs") {
        group = "build"
        description = "Copy pre-built native libraries to resources (for distribution)"

        val resourcesDir = file("src/main/resources/natives")

        doLast {
            // Clean and recreate natives directory
            delete(resourcesDir)
            resourcesDir.mkdirs()

            var copiedCount = 0
            platforms.forEach { platform ->
                val sourceFile = workspaceRoot.resolve("target/${platform.rustTarget}/release/${platform.libFileName}")
                val destDir = resourcesDir.resolve(platform.jnaPrefix)

                if (sourceFile.exists()) {
                    copy {
                        from(sourceFile)
                        into(destDir)
                    }
                    copiedCount++
                    logger.lifecycle("✓ Copied ${platform.jnaPrefix}/${platform.libFileName}")
                } else {
                    logger.warn("⚠ Native library not found: ${sourceFile.absolutePath}")
                    logger.warn("  This is expected if you haven't built for ${platform.rustTarget}")
                }
            }

            if (copiedCount == 0) {
                logger.warn("⚠ No native libraries were copied!")
                logger.warn("  Run 'cargo build --release --target <TARGET>' for each platform first,")
                logger.warn("  or let CI build them for you.")
            } else {
                logger.lifecycle("✓ Copied $copiedCount/${platforms.size} platform libraries")
            }
        }
    }

    // Clean native libraries from resources
    tasks.register("cleanNativeLibs") {
        group = "build"
        description = "Remove native libraries from resources"

        doLast {
            delete("src/main/resources/natives")
            logger.lifecycle("✓ Cleaned native libraries from resources")
        }
    }
}
