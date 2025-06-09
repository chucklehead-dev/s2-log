import dev.clojurephant.plugin.clojure.tasks.ClojureCompile
import org.gradle.jvm.tasks.Jar

plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.8.0"
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.serialization")
    `maven-publish`
}

group = "dev.chucklehead"
version = project.properties["version"]!!

val defaultJvmArgs = listOf(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Djdk.attach.allowAttachSelf",
    "-Darrow.memory.debug.allocator=false",
    "-XX:-OmitStackTraceInFastThrow",
    "-Dlogback.configurationFile=${rootDir.resolve("src/main/resources/logback-test.xml")}",
    "-Dxtdb.rootDir=$rootDir",
    "-Djunit.jupiter.extensions.autodetection.enabled=true"
)

repositories {
    mavenCentral()
    maven { url = uri("https://repo.clojars.org/") }
    maven { url = uri("https://maven.pkg.github.com/s2-streamstore/s2-sdk-java")
    credentials {
        username = System.getenv("GITHUB_ACTOR")
        password = System.getenv("GITHUB_TOKEN")
    }}
}

dependencies {
    implementation(libs.bundles.xtdb)
    implementation(libs.s2.sdk)
    implementation(libs.kotlinx.coroutines.guava)
    implementation(libs.clojure)
    testImplementation(kotlin("test"))
    nrepl("cider", "cider-nrepl", "0.50.1")
}

tasks.test {
    useJUnitPlatform()
}

java {
    withSourcesJar()
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Version" to project.version,
        )
    }
}

clojure {
    builds.forEach {
        it.checkNamespaces.empty()
    }
}

tasks.clojureRepl {
    doFirst {
        project.ext.set("buildEnv", "repl")
    }

    forkOptions.run {
        jvmArgs = defaultJvmArgs
    }

    middleware.add("cider.nrepl/cider-middleware")
}

tasks.withType(ClojureCompile::class) {
    forkOptions.run {
        jvmArgs = defaultJvmArgs
    }
}

val uberjar = tasks.register("uberjar", Jar::class) {
    archiveBaseName = "${project.name}-uber"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Implementation-Version"] = version
        attributes["Implementation-Title"] = "XTDB S2 Log - All-in-one"
//        attributes["Main-Class"] = "chucklehead.xtdb.s2.S2Log"
    }
    exclude("META-INF/*.RSA", "META-INF/*.SF", "META-INF/*.DSA")
    from(  configurations.runtimeClasspath.get().map ({ if (it.isDirectory)  it else zipTree(it) }) )
    with(tasks.jar.get() as CopySpec)
}

tasks {
    "build" {
        dependsOn("uberjar")
    }
}

publishing {
    publications {
        create<MavenPublication>("jar") {
            groupId = project.group.toString()
            artifactId = "s2-log"
            version = project.version.toString()
            artifact(tasks.jar)
            pom {
                name.set("S2 Log for XTDB")
                url.set("https://github.com/chucklehead-dev/s2-log")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                scm {
                    connection = "scm:git:git@github.com:chucklehead-dev/s2-log.git"
                    url = "https://github.com/chucklehead-dev/s2-log"
                }
            }
        }

        create<MavenPublication>("uberjar") {
            groupId = project.group.toString()
            artifactId = "s2-log-uberjar"
            version = project.version.toString()
            artifact(uberjar)
            pom {
                name.set("S2 Log for XTDB - All-in-one")
                url.set("https://github.com/chucklehead-dev/s2-log")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                scm {
                    connection = "scm:git:git@github.com:chucklehead-dev/s2-log.git"
                    url = "https://github.com/chucklehead-dev/s2-log"
                }
            }
        }

    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/chucklehead-dev/s2-log")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}