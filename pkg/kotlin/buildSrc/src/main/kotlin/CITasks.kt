import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.kotlin.dsl.register
import java.io.File

/**
 * Registers CI-specific tasks for building production artifacts.
 */
fun Project.registerCITasks() {
    registerOrganizeNativeLibsTask()
}

/**
 * Registers the `organizeNativeLibs` task that organizes downloaded GitHub Actions
 * artifacts into the resources directory structure expected by JNA.
 *
 * This task is intended to be run in CI after downloading build artifacts.
 * It expects artifacts in: downloaded-artifacts/native-{platform}/
 * And organizes them into: src/main/resources/natives/{platform}/
 */
private fun Project.registerOrganizeNativeLibsTask() {
    tasks.register("organizeNativeLibs") {
        group = "build"
        description = "Organize downloaded native library artifacts into resources (CI only)"

        val artifactsDir = projectDir.parentFile.parentFile.resolve("downloaded-artifacts")
        val resourcesDir = projectDir.resolve("src/main/resources/natives")

        doLast {
            if (!artifactsDir.exists()) {
                throw GradleException(
                    "Artifacts directory not found: ${artifactsDir.absolutePath}\n" +
                    "This task is intended for CI use after downloading artifacts.\n" +
                    "Ensure artifacts are downloaded to 'downloaded-artifacts/' first."
                )
            }

            // Clean and recreate natives directory
            project.delete(resourcesDir)
            resourcesDir.mkdirs()

            val artifactDirs = artifactsDir.listFiles { file ->
                file.isDirectory && file.name.startsWith("native-")
            }?.toList() ?: emptyList()

            if (artifactDirs.isEmpty()) {
                throw GradleException(
                    "No native-* artifact directories found in ${artifactsDir.absolutePath}"
                )
            }

            var copiedCount = 0
            artifactDirs.forEach { artifactDir ->
                val platform = artifactDir.name.removePrefix("native-")
                val destDir = resourcesDir.resolve(platform)

                logger.lifecycle("Processing platform: $platform")

                destDir.mkdirs()

                val libraryFiles = artifactDir.listFiles()?.toList() ?: emptyList()
                if (libraryFiles.isEmpty()) {
                    logger.warn("⚠ No files found in ${artifactDir.name}")
                } else {
                    libraryFiles.forEach { file ->
                        copy {
                            from(file)
                            into(destDir)
                        }
                        logger.lifecycle("  ✓ Copied ${file.name} → natives/$platform/")
                    }
                    copiedCount++
                }
            }

            logger.lifecycle("✓ Organized native libraries for $copiedCount platforms")

            // List final structure
            logger.lifecycle("\nNative libraries structure:")
            resourcesDir.listFiles()?.forEach { platformDir ->
                if (platformDir.isDirectory) {
                    platformDir.listFiles()?.forEach { lib ->
                        logger.lifecycle("  natives/${platformDir.name}/${lib.name}")
                    }
                }
            }
        }
    }
}
