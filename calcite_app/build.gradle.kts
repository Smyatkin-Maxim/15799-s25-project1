plugins {
    `java-library`
    `maven-publish`
    id("com.gradleup.shadow") version "8.3.5"
}

repositories {
    mavenLocal()
    maven {
        url = uri("https://repo.maven.apache.org/maven2/")
    }
}

val CALCITE_VERSION = properties.get("calcite.version")
dependencies {
    implementation("com.google.guava:guava")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-simple:2.0.16")
    implementation("org.apache.calcite:calcite-core:${CALCITE_VERSION}")
    implementation("org.apache.calcite:calcite-server:${CALCITE_VERSION}")
    implementation("org.duckdb:duckdb_jdbc:1.1.3")
    implementation("org.immutables:value:2.10.1")
    annotationProcessor("org.immutables:value:2.10.1")
}

group = "edu.cmu.cs.db"
version = "1.0-SNAPSHOT"
description = "calcite_app"
java.sourceCompatibility = JavaVersion.VERSION_1_8

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-Xlint:deprecation")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "edu.cmu.cs.db.calcite_app.app.App"
    }
}

publishing {
    publications.create<MavenPublication>("maven") {
        from(components["java"])
    }
}
