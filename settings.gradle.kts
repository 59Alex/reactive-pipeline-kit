rootProject.name = "reactive-pipeline-kit"
include("core", "jolt")

// The Java 17 build has no dependency on the optional Java 25 adapter.
if (providers.gradleProperty("java25").isPresent) {
    include("java25")
}
if (providers.gradleProperty("java27").isPresent) {
    include("java27")
}
