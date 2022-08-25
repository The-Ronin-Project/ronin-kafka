plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.pretty.jupiter)
    jacoco
}

dependencies {
    implementation(libs.kotlin.stdlib)
    implementation(libs.microutils.kotlin.logging)
    implementation(libs.kafka)
    implementation(libs.jackson)

    runtimeOnly(libs.logstash.logback)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.mockk)
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

tasks {
    test {
        testLogging.showStandardStreams = true
        testLogging.showExceptions = true
    }
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}
