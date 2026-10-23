plugins { base }

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    group = "io.github.59alex"
    version = "0.1.0"
    repositories { mavenCentral() }

    extensions.configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        withSourcesJar()
        withJavadocJar()
    }
    tasks.withType<JavaCompile>().configureEach {
        options.release.set(17)
        options.encoding = "UTF-8"
    }
    tasks.withType<Test>().configureEach { useJUnitPlatform() }
    tasks.withType<Jar>().configureEach {
        archiveBaseName.set(if (project.name == "core") "reactive-pipeline-kit" else "reactive-pipeline-kit-${project.name}")
        from(rootProject.file("LICENSE")) { into("META-INF") }
    }
    dependencies {
        "testImplementation"("org.junit.jupiter:junit-jupiter:5.11.4")
        "testImplementation"("io.projectreactor:reactor-test:3.7.0")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher:1.11.4")
    }
    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("library") {
                from(components["java"])
                artifactId = if (project.name == "core") "reactive-pipeline-kit" else "reactive-pipeline-kit-${project.name}"
                pom {
                    name.set("Reactive Pipeline ${project.name}")
                    description.set("Typed Reactor pipelines with bounded concurrency and per-step reliability policies")
                    url.set("https://github.com/59Alex/reactive-pipeline-kit")
                    licenses { license { name.set("MIT License"); url.set("https://opensource.org/licenses/MIT") } }
                    developers { developer { id.set("59Alex"); name.set("Aleksandr Kashevarov") } }
                    scm { url.set("https://github.com/59Alex/reactive-pipeline-kit"); connection.set("scm:git:https://github.com/59Alex/reactive-pipeline-kit.git") }
                }
            }
        }
    }
}
