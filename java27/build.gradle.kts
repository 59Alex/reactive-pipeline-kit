dependencies { "api"(project(":core")) }

val examples = sourceSets.create("examples") {
    compileClasspath += sourceSets.main.get().output + configurations.runtimeClasspath.get()
    runtimeClasspath += output + compileClasspath
}

tasks.register<JavaExec>("profileJava27") {
    group = "application"
    description = "Record a small pipeline with Java 27 JFR startup-data redaction"
    classpath = examples.runtimeClasspath
    mainClass.set("io.github.alex59.pipeline.java27.ProfileExample")
    javaLauncher.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(27))
    })
    // Keep the defaults and add an application-specific property name.
    jvmArgs("-XX:FlightRecorderOptions:redact-key=+pipeline.private.setting")
    systemProperty("pipeline.private.setting", "synthetic-example-value")
    // A demonstration recording must not collect the invoking shell's environment.
    setEnvironment(emptyMap<String, String>())
    args(layout.buildDirectory.file("recordings/pipeline.jfr").get().asFile.absolutePath)
}
