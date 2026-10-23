# reactive-pipeline-kit

Typed Reactor pipelines for record enrichment and transformation, with explicit
retry boundaries, bounded concurrency, and named step diagnostics.

[![Build](https://github.com/59Alex/reactive-pipeline-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/59Alex/reactive-pipeline-kit/actions/workflows/ci.yml)
[![Java 27+](https://img.shields.io/badge/Java-27%2B-437291)](https://github.com/59Alex/reactive-pipeline-kit)
[![MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

When a record passes validation, remote enrichment, and reshaping, a retry of
enrichment should not repeat the earlier steps. This library makes that boundary
part of the pipeline definition. It supplies the same execution identity to each
step and reports the step and attempt that failed.

## Modules

| Module | Purpose |
|---|---|
| `core` | Typed composition, conditions, per-attempt timeout, filtered retries, concurrent record processing, observer events, JDK HTTP and explicit blocking adapters |
| `jolt` | Isolated JSON-tree transformations using Jolt's built-in operations |

The core depends on Reactor, without Spring, reflection, scripting, or a runtime
configuration interpreter. Use normal Java configuration and your existing DI
container. The Jolt module is optional.

## Build and use

Version 0.2.0 requires JDK 27 or newer, without preview flags. The Gradle wrapper
is included. See the [Java 27 migration notes](docs/JAVA-27.md) when upgrading
from the Java 17-based 0.1.x release.

```sh
./gradlew build
./gradlew publishToMavenLocal
```

These coordinates are for the local publication produced by the command above:

```kotlin
repositories {
    mavenLocal()
    mavenCentral()
}
dependencies {
    implementation("io.github.59alex:reactive-pipeline-kit:0.2.0")
    // Optional JSON transformations:
    implementation("io.github.59alex:reactive-pipeline-kit-jolt:0.2.0")
}
```

## A typed pipeline

```java
import io.github.alex59.pipeline.*;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

var policy = new StepPolicy(
    Duration.ofSeconds(2),    // timeout for each attempt
    2,                        // at most 2 retries after the first attempt
    Duration.ofMillis(100),
    IOException.class::isInstance
);

Pipeline<String, String> pipeline = Pipeline.<String>start()
    .map("normalize", String::trim)
    .then("enrich", (value, context) -> Mono.just(value.toUpperCase(Locale.ROOT)), policy)
    .thenIf("prefix", value -> value.length() > 3,
        (value, context) -> Mono.just("item:" + value), StepPolicy.defaults());

Flux<String> results = pipeline.process(
    Flux.just(" java ", " ts "), 4, Ordering.INPUT
);
// Emits "item:JAVA", "TS" in input order when subscribed.
```

The example's enrichment is local; substitute your asynchronous service call for
`Mono.just(...)`. A step can change the output type. Conditional steps preserve
the type and pass the current value through when the predicate is false. Every
step must emit exactly one non-null result: an empty completion is a named
failure, not a silently dropped record. `map` is for fast, nonblocking work.

Definitions are immutable. Adding a step or observer returns a new definition.
Each subscription to `run(input)` executes again with a new identity; it does not
cache the result. Prefer immutable input values and thread-safe step functions.

## HTTP boundary

Use `HttpPort<Request, Response>` for an application-owned client. The supplied
JDK adapter avoids coupling the core to a particular web framework:

```java
import java.net.URI;
import java.net.http.*;

var http = HttpSteps.jdk(HttpClient.newHttpClient(), HttpResponse.BodyHandlers.ofString());
var fetch = Pipeline.<HttpRequest>start()
    .then("fetch", http.asStep(), StepPolicy.once(Duration.ofSeconds(5)))
    .map("body", HttpResponse::body);

var request = HttpRequest.newBuilder(URI.create("https://example.org/data")).GET().build();
Mono<String> body = fetch.run(request);
```

The application owns the HTTP client. The adapter passes cancellation to its
future; the remote endpoint may already have processed the request. HTTP 4xx/5xx
responses are returned as values. To retry a selected status, classify it as an
error *inside the HTTP step* and apply a retry policy to that step. A later
validation error does not rerun an earlier HTTP request. The chosen body handler
determines buffering; `ofString()` loads the response body into memory.

## Blocking I/O on virtual threads

Keep asynchronous clients on the normal `Step` API. For a blocking driver or
legacy call, `BlockingSteps.call` runs each attempt on an executor supplied by the
application. A virtual-thread-per-task executor is useful for blocking I/O:

```java
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;

// Application entry point; the application owns the executor's lifecycle.
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    var readFile = Pipeline.<Path>start()
        .then("read", BlockingSteps.call(executor, Files::readString));

    String contents = readFile.run(Path.of("document.txt"))
        .block(Duration.ofSeconds(5));
}
```

The final `block` is appropriate only at a synchronous application boundary,
not on a Reactor event-loop thread. In a reactive application, keep the executor
alive for the application's lifetime and close it during shutdown. The library
never creates or closes it, and does not install an implicit scheduler.

Inside the blocking operation, `BlockingSteps.currentContext()` exposes its
`ExecutionContext` through a scoped value. It is empty outside the synchronous
operation, including downstream signals and unrelated executor tasks. Reactor
context remains the mechanism for asynchronous propagation; scoped values do
not automatically follow scheduler hops.

Cancellation and attempt timeout cancel the `FutureTask` with interruption
requested. An interruptible operation can stop; an operation that ignores
interrupts can keep running, even while a retry starts. Use idempotency for
external effects. `process(..., concurrency, ...)` still limits subscribed
records; virtual threads do not impose that limit themselves. Do not supply an
executor that runs blocking tasks inline on the subscribing thread.

## Diagnostics and subscriber context

```java
var observed = pipeline.observe(event -> {
    // Send event.step(), event.kind(), event.attempt(), event.elapsed()
    // and event.executionId() to your logging or metrics system.
});

observed.run("java")
    .contextWrite(context -> context.put("tenant", "example"));
// A step can read context.subscriberContext().get("tenant").
```

Observers receive `STARTED`, `SUCCEEDED`, `FAILED`, `CANCELLED`, or `SKIPPED`.
Retries produce separate attempt events. Observers must be nonblocking and
thread-safe. Observer exceptions go to Reactor's `onErrorDropped` hook and do
not retry business work. Events omit input/output payloads; exception objects
can still contain application data. `StepFailure` preserves the original cause,
step name, and final attempt count. Predicate failures have attempt count zero.

## Optional Jolt transformation

```java
import io.github.alex59.pipeline.jolt.JoltStep;
import java.util.List;
import java.util.Map;

var reshape = JoltStep.fromSpec(List.of(
    Map.of("operation", "shift", "spec", Map.of("id", "record.key"))
));
var json = Pipeline.<Object>start().then("reshape", reshape);
// json.run(Map.of("id", 7)) emits {record={key=7}}.
```

Supported operations: `shift`, `default`, `remove`, `cardinality`, `sort`.
The adapter snapshots the specification and copies JSON-compatible input trees.
Each attempt creates its own Jolt instance; neither a mutable input tree nor
transform state is shared by the adapter. Custom transform class names and script
execution are excluded. Trees deeper than 128 levels and non-JSON object values
are rejected.

Jolt materializes the complete tree. This module does **not** parse or transform
one JSON document as a byte stream. CPU transformations run on the subscribing
thread; explicitly choose a scheduler for large documents. Per-attempt Jolt
compilation is an isolation tradeoff, not a throughput optimization.

## Reliability contract

| Behavior | Boundary |
|---|---|
| Retry | Only the failing step, only accepted errors; disabled by default |
| Timeout | Per attempt, 30 seconds by default; retry delay is additional time |
| Cancellation | Propagates upstream and to active work; no rollback of external effects |
| Concurrency | At most the configured number of subscribed records; explicit blocking tasks use the application's executor |
| `Ordering.INPUT` | Keeps input order, buffering later completed records behind a slow record |
| `Ordering.COMPLETION` | Emits completed records without waiting for earlier ones |
| Failure | Fails the processing stream and cancels sibling work; already emitted results remain emitted |
| State | New execution ID and retry counters per run subscription; no deep copy of caller values |

Ordinary `Step.apply` callbacks must return promptly; put blocking operations
behind the explicit adapter. A Reactor timeout
cannot forcibly stop blocking Java code or undo a server-side write. Retries can
repeat a side effect after an ambiguous failure; use idempotency keys where
needed. Retried steps receive the same input reference, so mutating it can change
the next attempt. The library offers no durable execution log, restart recovery,
distributed transaction, or exactly-once effect guarantee.

Backpressure bounds active records, not total bytes. A single large payload,
an eager source, an application cache, or an HTTP body handler can consume more
memory. See [architecture and failure scenarios](docs/ARCHITECTURE.md).

## Verification

Tests cover retry isolation and exhaustion, cancellation of active siblings,
timeout cancellation and delayed retry, concurrency limits and replenishment,
output ordering, per-subscription identity and retry budget, conditions, observer
failure isolation, a real local HTTP exchange, and concurrent Jolt transformations.
Blocking-adapter tests use real virtual threads and verify scoped-context
isolation, interruption, cancellation during executor handoff, retry boundaries,
and executor rejection. JSON tests preserve numeric types and reject nonfinite
or unsupported numbers. CI builds on JDK 27. Tests use Reactor virtual time where
timing is part of the asynchronous contract.

## License

[MIT](LICENSE). Dependency licenses are listed in [third-party notices](THIRD-PARTY-NOTICES.md).
