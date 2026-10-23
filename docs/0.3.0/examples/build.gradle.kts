plugins { application }

val exampleName = providers.gradleProperty("example").getOrElse("Example")
val minimumJava = when (exampleName) {
    "Example", "JoltExample" -> 17
    "ScopedExample" -> 25
    "JfrExample" -> 27
    else -> error("Unknown example: $exampleName. Choose Example, JoltExample, ScopedExample or JfrExample.")
}
val exampleJava = providers.gradleProperty("exampleJava").map(String::toInt)
    .getOrElse(maxOf(minimumJava, JavaVersion.current().majorVersion.toInt()))
require(exampleJava >= minimumJava) { "$exampleName requires Java $minimumJava or newer" }

repositories {
    mavenLocal()
    mavenCentral()
}
dependencies {
    implementation("io.github.59alex:reactive-pipeline-kit:0.3.0")
    when (exampleName) {
        "JoltExample" -> implementation("io.github.59alex:reactive-pipeline-kit-jolt:0.3.0")
        "ScopedExample" -> implementation("io.github.59alex:reactive-pipeline-kit-java25:0.3.0")
        "JfrExample" -> implementation("io.github.59alex:reactive-pipeline-kit-java27:0.3.0")
    }
}
java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(exampleJava))
}
sourceSets.main {
    java.setSrcDirs(listOf("."))
    java.include("$exampleName.java")
}
tasks.withType<JavaCompile>().configureEach {
    options.release.set(minimumJava)
    options.encoding = "UTF-8"
}
application { mainClass.set(exampleName) }
tasks.named<JavaExec>("run") {
    if (exampleName == "JfrExample") {
        args(layout.buildDirectory.file("recordings/example.jfr").get().asFile.absolutePath)
    }
}
