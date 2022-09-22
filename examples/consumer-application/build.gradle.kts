plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.pretty.jupiter)
    alias(libs.plugins.ktlint)
}

dependencies {
    implementation(project(":lib"))
    implementation(project(":examples:data"))

    implementation(libs.kotlin.stdlib)
    implementation(libs.microutils.kotlin.logging)
    implementation(libs.jackson)
    implementation(libs.kafka)
    implementation(libs.micrometer)

    runtimeOnly(libs.logstash.logback)
    runtimeOnly(libs.logback.classic)
}
