# Java support and migration to 0.3.0

[← Back to documentation and versions / Назад к документации и версиям](index.md)

The default library works on Java 17. Core and Jolt are tested on LTS runtimes
**17, 21, and 25**, and also on **Java 27**. Scoped context and JFR diagnostics
are separate opt-in artifacts. No module requires preview features or flags.

| Artifact | Minimum Java | Purpose |
|---|---|---|
| `reactive-pipeline-kit` | 17 | Reactor pipelines, HTTP adapter, blocking adapter with explicit context |
| `reactive-pipeline-kit-jolt` | 17 | JSON-tree transformations |
| `reactive-pipeline-kit-java25` | 25 | Optional ScopedValue binding, tested on 25 and 27 |
| `reactive-pipeline-kit-java27` | 27 | Optional JFR step diagnostics and startup-redaction profile |

All coordinates use group `io.github.59alex` and version `0.3.0`. Java packages
remain `io.github.alex59.pipeline`; optional adapters use the `.java25` and
`.java27` subpackages. Neither core nor Jolt depends on those adapters, so no
Java 25/27 classes are pulled into Java 17/21 applications.

## Build selection

The default build includes only core and Jolt and can run on JDK 17:

```sh
./gradlew build
./gradlew publishToMavenLocal
```

Select the runtime used for core/Jolt tests when verifying the runtime matrix:

```sh
./gradlew build -PtestJavaVersion=17
./gradlew build -PtestJavaVersion=21
./gradlew build -PtestJavaVersion=25
./gradlew build -PtestJavaVersion=27
```

The selected JDK must be installed and discoverable by Gradle. The public core
and Jolt bytecode/API baseline remains Java 17 regardless of test runtime.
Enable the optional module explicitly on a machine with JDK 25:

```sh
./gradlew -Pjava25 -PtestJavaVersion=25 build publishToMavenLocal
```

The `java25` adapter has Java 25 bytecode and can be compiled/tested with a
selected JDK of 25 or newer. Enable the Java 27 profile independently, or combine
both adapters on Java 27:

```sh
./gradlew -Pjava27 -PtestJavaVersion=27 build
./gradlew -Pjava25 -Pjava27 -PtestJavaVersion=27 build
./gradlew -Pjava27 -PtestJavaVersion=27 :java27:profileJava27
```

Both adapters are omitted from the default build. A Java 17 consumer needs no
Java 25/27 installation to build or use core and Jolt. The Java 27 observer uses
established JFR event APIs; its optional runtime profile demonstrates the
Java 27 startup-redaction feature rather than raising the core's baseline. See
[JFR on Java 27](JFR-JAVA27.md).

## Blocking I/O and virtual threads

The core accepts an `ExecutorService` owned by the application:

```java
Step<String, String> lookup = BlockingSteps.call(executor, (input, context) -> {
    String tenant = context.subscriberContext().get("tenant");
    return tenant + ":" + input;
});
```

Use a suitable platform-thread executor on Java 17. On Java 21/25/27 the application
can pass `Executors.newVirtualThreadPerTaskExecutor()` to the same core method.
Virtual threads require no optional library module and no runtime reflection in
the production core. The application creates and closes the executor; the core
does not implicitly change schedulers or execute work in a managed pool of its
own.

Cancellation cancels a task with interruption requested, including cancellation
during executor handoff. `FutureTask.done()` reads a completed result using the
Java 17 API and unwraps `ExecutionException`. Checked failures retain their
original cause in `StepFailure`. Cancellation cannot force noncooperative code to
stop or undo a remote side effect. Pipeline concurrency bounds subscriptions,
not abandoned I/O that ignored interruption.

## Optional scoped context

Applications on Java 25 or 27 may add
`io.github.59alex:reactive-pipeline-kit-java25:0.3.0` and use:

```java
Step<String, String> lookup = ScopedBlockingSteps.call(executor, input -> {
    ExecutionContext context = ScopedBlockingSteps.currentContext().orElseThrow();
    return context.subscriberContext().getOrDefault("prefix", "") + input;
});
```

The wrapper delegates to the core blocking adapter and binds a `ScopedValue`
only around the synchronous operation. Binding cleanup occurs on success and
exception before downstream notification. Unrelated threads and Reactor
scheduler hops do not inherit that binding. Use Reactor context for asynchronous
propagation. The core itself has no ambient `currentContext()` method.

## Migrating from 0.2.0

- Replace the former Java 27 requirement with the appropriate artifact minimum
  above. Upgrade all used artifacts together to 0.3.0.
- Change a core blocking lambda from `input -> ...` to `(input, context) -> ...`
  and replace `BlockingSteps.currentContext().orElseThrow()` with that explicit
  `context` parameter.
- If scoped lookup is needed and the application runs Java 25 or 27, add the optional
  artifact and replace `BlockingSteps` with `ScopedBlockingSteps` for those
  operations. Other pipelines can continue using only the core.
- Existing pipeline retry, timeout, ordering, cancellation, and error semantics
  remain unchanged. The JSON scalar whitelist continues to preserve exact
  numeric types and reject nonfinite values and custom Number implementations,
  including BigInteger/BigDecimal subclasses.

Jolt's copy routine uses Java 17-compatible pattern `instanceof`, without
guarded switch or preview syntax. Its input copying, null handling, maximum
depth, and built-in operation allowlist are unchanged from 0.2.0.

## Verification boundaries

Core tests compile for Java 17. Test-only reflection selects and verifies real
virtual threads on 21/25/27, while the same behavior is checked with platform
threads on 17. Scoped-value tests are confined to the optional Java 25 module
and run on 25/27. Java 27 diagnostics tests record and read actual JFR events,
including synthetic startup properties in isolated child JVMs.
The tests cover context isolation, retry scope, timeout/interruption, handoff
cancellation races, concurrency, rejection, HTTP behavior, and Jolt isolation.

The required class-file versions are 61.0 for core/Jolt, 69.0 for `java25`, and
71.0 for `java27`. Publication metadata declares minimum JVM versions 17, 25,
and 27 respectively. There is no core-to-optional-module dependency and no preview
class-file marker.

[← Back to documentation and versions / Назад к документации и версиям](index.md)
