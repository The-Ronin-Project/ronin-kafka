plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.ktlint)
}

dependencies {
    implementation(libs.kotlin.stdlib)
    implementation(rootProject)
}
