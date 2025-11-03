import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.kotlin.dsl.register
import java.io.File

/**
 * Registers Rust build and UniFFI binding generation tasks.
 *
 * @param workspaceRoot Root directory of the Cargo workspace
 */
fun Project.registerRustBuildTasks(workspaceRoot: File) {
    registerBuildRustLibTask(workspaceRoot)
    registerGenerateBindingsTask(workspaceRoot)
}

/**
 * Registers the `buildRustLib` task that compiles Rust code for the local platform.
 * This task runs in debug mode for faster development builds.
 */
private fun Project.registerBuildRustLibTask(workspaceRoot: File) {
    tasks.register("buildRustLib") {
        group = "build"
        description = "Build the Rust library for local platform (debug mode)"

        val debugLibPath = workspaceRoot.resolve(
            "target/debug/${currentPlatformLibName()}.${currentPlatformLibExtension()}"
        )
        outputs.file(debugLibPath)

        // Only rebuild if source files changed
        inputs.files(fileTree(projectDir.resolve("src")).include("**/*.rs"))
        inputs.file(projectDir.resolve("Cargo.toml"))

        doLast {
            logger.lifecycle(
                "Building Rust library for ${currentPlatformLibName()}.${currentPlatformLibExtension()}..."
            )

            @Suppress("DEPRECATION")
            val result = project.exec {
                workingDir = projectDir
                commandLine("cargo", "build")
                isIgnoreExitValue = true
            }

            if (result.exitValue != 0) {
                throw GradleException(
                    "Rust build failed with exit code ${result.exitValue}. " +
                    "Check the output above for details."
                )
            }

            if (!debugLibPath.exists()) {
                throw GradleException(
                    "Rust library not found at: ${debugLibPath.absolutePath}\n" +
                    "Expected library name: ${currentPlatformLibName()}.${currentPlatformLibExtension()}\n" +
                    "Make sure Cargo.toml has the correct library name configuration."
                )
            }

            logger.lifecycle("✓ Built Rust library: ${debugLibPath.name}")
        }
    }
}

/**
 * Registers the `generateBindings` task that runs UniFFI to generate Kotlin bindings
 * from the Rust library.
 *
 * Supports custom library path via project property:
 * - Default: Uses debug build (target/debug/)
 * - With `-PlibPath=path`: Uses specified library path (for CI)
 */
private fun Project.registerGenerateBindingsTask(workspaceRoot: File) {
    tasks.register("generateBindings") {
        group = "build"
        description = "Generate UniFFI Kotlin bindings from Rust library"

        val generatedDir = layout.buildDirectory.dir("generated/source/uniffi/kotlin").get().asFile

        // Allow custom library path via -PlibPath=... (for CI)
        val customLibPath = project.findProperty("libPath") as String?
        val libPath = if (customLibPath != null) {
            File(customLibPath).also {
                logger.lifecycle("Using custom library path: ${it.absolutePath}")
            }
        } else {
            // Default: debug build for local development
            dependsOn("buildRustLib")
            workspaceRoot.resolve(
                "target/debug/${currentPlatformLibName()}.${currentPlatformLibExtension()}"
            )
        }

        inputs.files(fileTree(projectDir.resolve("src")).include("**/*.rs", "**/*.udl"))
        outputs.dir(generatedDir)

        doLast {
            generatedDir.mkdirs()

            if (!libPath.exists()) {
                throw GradleException(
                    "Rust library not found at: ${libPath.absolutePath}\n" +
                    if (customLibPath != null) {
                        "Ensure the library is built and the path is correct."
                    } else {
                        "Run './gradlew buildRustLib' first to build the library."
                    }
                )
            }

            logger.lifecycle("Generating UniFFI Kotlin bindings from ${libPath.name}...")

            @Suppress("DEPRECATION")
            val result = project.exec {
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
                    libPath.absolutePath,
                    "--no-format",
                )
                isIgnoreExitValue = true
            }

            if (result.exitValue != 0) {
                throw GradleException(
                    "UniFFI binding generation failed with exit code ${result.exitValue}. " +
                    "Check the output above for details."
                )
            }

            val generatedFiles = generatedDir.listFiles()?.size ?: 0
            logger.lifecycle("✓ Generated $generatedFiles Kotlin binding file(s) in ${generatedDir.name}")
        }
    }
}
