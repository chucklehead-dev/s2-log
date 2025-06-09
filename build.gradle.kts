import dev.clojurephant.plugin.clojure.tasks.ClojureCompile

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
    maven {
        url = uri("https://maven.pkg.github.com/s2-streamstore/s2-sdk-java")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation(libs.bundles.xtdb)
    implementation(libs.s2.sdk)
    implementation(libs.kotlinx.coroutines.guava)
    implementation(libs.clojure)
    implementation(libs.kotlinx.coroutines)
//    implementation(libs.grpc)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(kotlin("test"))
    testImplementation(libs.mockk)
    nrepl("cider", "cider-nrepl", "0.50.1")
}

tasks.test {
    jvmArgs(defaultJvmArgs)
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register("integration-test", Test::class) {
    jvmArgs(defaultJvmArgs)
    useJUnitPlatform {
        includeTags("integration")
    }
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
tasks.shadowJar {
    minimize()
    mergeServiceFiles()
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

publishing {
    publications {
        create<MavenPublication>("jar") {
            artifactId = "s2-log"
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

//        create<MavenPublication>("uberjar") {
//            artifactId = "s2-log-all"
//            from(components["shadow"])
//        }

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