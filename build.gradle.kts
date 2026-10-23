plugins { base }

val testJavaVersion = providers.gradleProperty("testJavaVersion").map(String::toInt)
    .getOrElse(JavaVersion.current().majorVersion.toInt())

subprojects {
    val minimumJava = when (project.name) {
        "java27" -> 27
        "java25" -> 25
        else -> 17
    }
    val executionJava = maxOf(minimumJava, testJavaVersion)
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    group = "io.github.59alex"
    version = "0.3.0"
    repositories { mavenCentral() }

    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(executionJava))
        sourceCompatibility = JavaVersion.toVersion(minimumJava)
        targetCompatibility = JavaVersion.toVersion(minimumJava)
        withSourcesJar()
        withJavadocJar()
    }
    tasks.withType<JavaCompile>().configureEach {
        options.release.set(minimumJava)
        options.encoding = "UTF-8"
    }
    tasks.withType<Test>().configureEach { useJUnitPlatform() }
    tasks.withType<Jar>().configureEach {
        archiveBaseName.set(if (project.name == "core") "reactive-pipeline-kit" else "reactive-pipeline-kit-${project.name}")
        from(rootProject.file("LICENSE")) { into("META-INF") }
    }
    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:6.1.3"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testImplementation"("io.projectreactor:reactor-test:3.8.8")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
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
