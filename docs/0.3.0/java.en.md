# reactive-pipeline-kit 0.3.0 — Java: compatibility and adopted features

[English](java.en.md) · [Русский](java.ru.md)<br>
[Usage guide](en.md) · [Library versions](../README.md)

[Java 17](#java-17) · [Java 21](#java-21) · [Java 25](#java-25) · [Java 27](#java-27)

Library and Java versions are different. The base 0.3.0 artifacts target Java 17. Running on a newer JVM does not mean the library adopts every new API or provides a measured performance improvement.

## Java 17

Minimum core version. Compiled with --release 17, class-file 61.0, without preview. All baseline capabilities in the usage guide are available here.

```sh
./gradlew build -PtestJavaVersion=17
```

## Java 21

Java 21 lets the application supply a virtual-thread executor to the core. BlockingSteps accepts the ordinary ExecutorService interface, preserving the Java 17 baseline. The application creates and closes the executor; concurrency limits remain explicit.

```java
var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
try {
    var step = io.github.alex59.pipeline.BlockingSteps.<String, String>call(
        executor, (input, context) -> input.trim());
    var result = io.github.alex59.pipeline.Pipeline.<String>start()
        .then("normalize", step).run(" hello ").block();
    System.out.println(result); // hello
} finally {
    executor.shutdownNow();
}
```

Place this in a Java 21+ application main method. Virtual threads suit blocking I/O; the example operation is deliberately small. Cancellation requests interruption, but does not roll back external effects.

```sh
./gradlew build -PtestJavaVersion=21
```

## Java 25

The optional reactive-pipeline-kit-java25 module uses ScopedValue for execution context within a synchronous blocking operation. The binding is cleaned up on success and failure; it does not automatically cross asynchronous Reactor scheduler hops. The module requires Java 25, the core still requires 17.

```sh
./gradlew -Pjava25 -PtestJavaVersion=25 build publishToMavenLocal
```

[Complete ScopedValue example](examples/ScopedExample.java) · [Usage guide](en.md)

## Java 27

The optional reactive-pipeline-kit-java27 module adds JfrStepObserver and a profiling example using JDK 27 startup-data redaction (JEP 536). The observer uses stable JFR APIs; the new filters are JVM startup options. They do not sanitize arbitrary custom event fields: step names must be non-sensitive. The observer excludes payloads and exception messages.

```sh
./gradlew -Pjava27 -PtestJavaVersion=27 build :java27:profileJava27
```

[Complete JFR example](examples/JfrExample.java) · [Redaction boundaries](../JFR-JAVA27.md)

## Changes between library versions

| Version | Java requirements and capabilities |
| --- | --- |
| 0.1.0 | Java 17 baseline. |
| 0.2.0 | Historical release requiring Java 27; no longer the current recommendation. |
| 0.3.0 | Java 17 restored; Java 21 virtual threads via an application executor, ScopedValue separated into java25, JFR into java27. |

[Compatibility and migration](../JAVA-SUPPORT.md)
