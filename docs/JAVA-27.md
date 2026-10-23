# Java 27 and version 0.2.0

Version 0.2.0 moves the build and runtime baseline from Java 17 to Java 27.
The compiled artifacts require Java 27 or newer. No `--enable-preview` flag is
needed. Update both modules together if the application uses the Jolt adapter.

## Features used and their purpose

| Stable Java feature | Application in this library |
|---|---|
| Pattern matching for switch, guarded patterns, `case null` | Jolt's defensive JSON-tree copy names each accepted type explicitly and rejects unsupported/nonfinite values |
| Virtual threads | Application-owned executors can run cooperative blocking I/O through `BlockingSteps.call` without occupying a platform thread for each wait |
| Scoped values | Execution metadata is available to nested synchronous blocking calls without mutable ThreadLocal cleanup; the binding ends when the operation returns or throws |
| Future terminal-state inspection | `FutureTask.done()` reports results/errors without a blocking `get()` or an ExecutionException wrapper |

These are stable APIs available on Java 27; the first three were introduced in
earlier releases rather than invented for Java 27. Pattern switch and virtual
threads became final in Java 21; scoped values became final in Java 25. The
upgrade uses them where they clarify behavior, without changing Reactor's role
as the asynchronous composition and backpressure layer.

The Jolt switch preserves `Byte`, `Short`, `Integer`, `Long`, `BigInteger`,
`BigDecimal`, finite `Float`, and finite `Double` values without converting them
to a common floating-point type. It also preserves JSON nulls. Unsupported
objects, nonfinite numbers, non-string map keys, and trees exceeding depth 128
still fail. Version 0.2.0 also tightens the scalar whitelist to reject custom
BigInteger/BigDecimal subclasses, which could carry mutable state; use standard
instances of those classes. Other custom Number implementations remain rejected.
Operation class loading remains restricted to the existing built-in operation
allowlist.

## Migration

1. Upgrade build agents, development JDKs, and deployment runtimes to Java 27.
2. Use the included Gradle wrapper and replace `0.1.0` with `0.2.0` in both Maven
   coordinates. Java package names stay `io.github.alex59.pipeline`.
3. Keep existing nonblocking `Step` and HTTP definitions unchanged. The existing
   retry, timeout, ordering, and error contracts remain in force.
4. Adapt genuinely blocking calls with `BlockingSteps.call(executor, operation)`.
   The application owns the executor and closes it only after its users finish.
5. Continue using Reactor context across asynchronous boundaries. Read
   `BlockingSteps.currentContext()` only when execution is inside a blocking
   operation; outside it, the method returns `Optional.empty()`.

Virtual threads improve the suitability of blocking I/O; they do not accelerate
CPU-heavy transformations or provide an automatic concurrency limit. Use the
pipeline's explicit record concurrency setting. Timeouts request interruption
but cannot stop a noncooperative operation or undo an external write. Such work
can overlap a later retry, so idempotency remains important.

## Why no structured concurrency or preview requirement

This pipeline already has an asynchronous lifecycle defined by Reactor
subscriptions, demand, cancellation, and retry. Replacing that lifecycle with a
task scope would change its contracts. Structured concurrency is therefore not
part of this migration, and no preview API is exposed to consumers. Scoped
values are used only for a bounded synchronous call within an explicitly
submitted task; their binding is not advertised as crossing Reactor scheduler
hops or unrelated executor submissions.

## Verification

The tests exercise actual virtual threads with `Thread.isVirtual()`, concurrent
context isolation, absence of scoped bindings outside the operation and after
exceptions, cancellation-induced interruption, cancellation during task handoff,
executor rejection, retry isolation, and the processing concurrency bound.
Existing asynchronous pipeline and HTTP tests continue to run. JSON-copy tests
cover exact numeric types, nested nulls, nonfinite values, unsupported objects,
and the existing depth/cycle rejection.

## References

- [ScopedValue API and dynamic scope](https://docs.oracle.com/en/java/javase/27/docs/api/java.base/java/lang/ScopedValue.html)
- [FutureTask lifecycle](https://docs.oracle.com/en/java/javase/27/docs/api/java.base/java/util/concurrent/FutureTask.html)
- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444)
- [JEP 441: Pattern Matching for switch](https://openjdk.org/jeps/441)
- [JEP 506: Scoped Values](https://openjdk.org/jeps/506)
