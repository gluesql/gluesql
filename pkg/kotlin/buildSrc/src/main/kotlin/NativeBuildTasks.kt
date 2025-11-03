import org.gradle.api.Project

/**
 * Main entry point for registering all native build-related Gradle tasks.
 *
 * This function registers Rust build tasks for local development:
 * - buildRustLib: Builds Rust library in debug mode for testing
 * - generateBindings: Generates Kotlin bindings from Rust using UniFFI
 *
 * Production builds for multiple platforms are handled by GitHub Actions.
 *
 * Call this from build.gradle.kts to set up the native build pipeline.
 */
fun Project.registerNativeBuildTasks() {
    val workspaceRoot = projectDir.parentFile.parentFile
    registerRustBuildTasks(workspaceRoot)
}
