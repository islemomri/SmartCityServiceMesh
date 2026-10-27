plugins {
    kotlin("jvm") version "2.4.20"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")
    implementation("org.eclipse.californium:californium-core:3.14.0")
    implementation("com.google.code.gson:gson:2.11.0")

}

kotlin {
    jvmToolchain(23)
}

tasks.test {
    useJUnitPlatform()
}