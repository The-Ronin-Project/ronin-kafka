import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.net.URL

@Suppress("DSL_SCOPE_VIOLATION")
plugins {
    // intellij shows an error with this, but it's fine: https://youtrack.jetbrains.com/issue/KTIJ-19369
    alias(libs.plugins.kotlin)
    alias(libs.plugins.pretty.jupiter)
    jacoco
    alias(libs.plugins.ktlint)
    `maven-publish`
    alias(libs.plugins.dokka)
}

allprojects {
    group = "com.projectronin"
    version = "1.0.0"

    repositories {
        maven {
            url = uri("https://repo.devops.projectronin.io/repository/maven-public/")
        }
    }

    tasks.withType<JavaCompile> {
        sourceCompatibility = "11"
        targetCompatibility = "11"
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions {
            freeCompilerArgs = listOf("-Xjsr305=strict")
            jvmTarget = "11"
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api(libs.kafka)
    api(libs.kafka.streams)
    api(libs.jackson)
    api(libs.micrometer)

    implementation(libs.kotlin.stdlib)
    implementation(libs.microutils.kotlin.logging)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.module.parameterNames)

    runtimeOnly(libs.logstash.logback)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.hamcrest)
    testImplementation(libs.mockk)
    testImplementation(libs.logstash.logback)
    testImplementation(libs.kotlinx.coroutines)
    testRuntimeOnly(libs.logback.classic)
}

jacoco {
    toolVersion = "0.8.8"
    // Custom reports directory can be specfied like this:
    reportsDirectory.set(layout.buildDirectory.dir("./codecov"))
}

tasks.jacocoTestReport {
    reports {
        xml.required.set(true)
        csv.required.set(false)
        html.required.set(false)
    }
}

tasks.test {
    testLogging.showStandardStreams = true
    testLogging.showExceptions = true
    finalizedBy(tasks.jacocoTestReport)
}

tasks.dokkaHtml.configure {
    outputDirectory.set(buildDir.resolve("dokka/html"))

    dokkaSourceSets {
        configureEach {
            includes.from("README.md")

            externalDocumentationLink {
                url.set(URL("https://kafka.apache.org/33/javadoc/"))
            }

            sourceLink {
                // Unix based directory relative path to the root of the project (where you execute gradle respectively).
                localDirectory.set(file("src/main/kotlin"))
                // URL showing where the source code can be accessed through the web browser
                remoteUrl.set(
                    URL(
                        "https://github.com/projectronin/ronin-kafka/blob/main/ronin-kafka/src/main/kotlin/"
                    )
                )
                // Suffix which is used to append the line number to the URL. Use #L for GitHub
                remoteLineSuffix.set("#L")
            }
        }
    }
}

publishing {
    repositories {
        maven {
            name = "nexus"
            credentials {
                username = System.getenv("NEXUS_USER")
                password = System.getenv("NEXUS_TOKEN")
            }
            url = if (project.version.toString().endsWith("SNAPSHOT")) {
                uri("https://repo.devops.projectronin.io/repository/maven-snapshots/")
            } else {
                uri("https://repo.devops.projectronin.io/repository/maven-releases/")
            }
        }
    }
    publications {
        create<MavenPublication>("library") {
            from(components["java"])
        }
    }
}
