# JFR diagnostics profile on Java 27

[← Back to documentation and versions / Назад к документации и версиям](index.md)

The opt-in `reactive-pipeline-kit-java27` module exposes `JfrStepObserver` and a
local profiling example. Core and Jolt remain Java 17 libraries. The observer
uses established JFR APIs; this separate runtime profile demonstrates Java 27's
in-process startup-data redaction without changing the baseline of every
consumer.

## Record a synthetic example

With JDK 27 installed, run:

```sh
./gradlew -Pjava27 -PtestJavaVersion=27 :java27:profileJava27
```

The task runs `io.github.alex59.pipeline.java27.ProfileExample` from the examples
source set and writes `java27/build/recordings/pipeline.jfr`. It processes 100
synthetic records through two named steps. The child process has a cleared
environment and only a harmless demonstration property. No environment-variable
events are enabled.

The example owns its `Recording` lifecycle and enables the custom step event
plus `jdk.InitialSystemProperty`. Its startup option adds a property filter
while retaining the JVM defaults:

```text
-XX:FlightRecorderOptions:redact-key=+pipeline.private.setting
```

The synthetic `pipeline.private.setting` value should appear as `[REDACTED]`.
Inspect only the relevant events locally, for example:

```sh
"$JAVA_HOME/bin/jfr" print --events io.github.alex59.pipeline.Step,jdk.InitialSystemProperty \
  java27/build/recordings/pipeline.jfr
```

## Connect an application recording

After building with `-Pjava27`, the local-publication coordinate is
`io.github.59alex:reactive-pipeline-kit-java27:0.3.0`. The application decides when
to start, stop, and export a recording:

```java
import io.github.alex59.pipeline.java27.JfrStepObserver;
import java.nio.file.Path;
import jdk.jfr.Recording;

try (Recording recording = new Recording()) {
    recording.enable(JfrStepObserver.EVENT_NAME).withoutStackTrace();
    recording.start();
    pipeline.observe(new JfrStepObserver()).run(input).block();
    recording.stop();
    recording.dump(Path.of("pipeline.jfr"));
}
```

The blocking terminal operation belongs at a synchronous application boundary.
Reactive services can instead manage a longer-lived recording through their
normal lifecycle. The observer itself has no recording lifecycle or JVM-option
management API.

## Event contract and redaction boundary

The event name is `io.github.alex59.pipeline.Step`. Its custom fields are:

| Field | Content |
|---|---|
| `executionId` | Execution UUID as text |
| `step` | The configured step name |
| `kind` | STARTED, SUCCEEDED, FAILED, CANCELLED, or SKIPPED |
| `attempt` | Attempt number, including zero for a skipped/failed condition |
| `elapsedNanos` | Attempt elapsed time in nanoseconds |
| `errorType` | Exception class name, or empty for a non-error event |

Payloads, return values, and exception messages are not copied. Standard JFR
timestamp/thread metadata still exists. Stack traces are disabled by the event
definition, although an application's recording configuration can override that
setting. Use static, non-sensitive step names: the observer copies names as given.

Java 27 [JEP 536](https://openjdk.org/jeps/536) redacts matching command-line
arguments and initial system-property/environment-variable values in built-in
startup events. Defaults include sensitive key patterns such as token, password,
and secret. Filters are JVM startup options; `+` extends the defaults instead of
replacing them. Multiple filters can be separated by semicolons, with the whole
option quoted when invoked through a shell.

**These startup filters do not redact custom event fields.** They are not a
general sanitizer for arbitrary payloads or every event another recording may
enable. `JfrStepObserver` therefore excludes business data at the source rather
than relying on a filter to remove it. The library does not mutate VM flags or
pretend there is a new per-observer Java redaction API.

## What this profile does not claim

This module does not change G1 settings, compact-object-header options, or TLS
provider behavior. Those are JVM/runtime choices, not pipeline-library API
features. No throughput, latency, allocation, or cryptographic improvement is
claimed from switching runtime versions. Measure the real application before
drawing performance conclusions. No preview, Vector, or unrelated crypto API is
introduced just to increase the Java version.

## Verification

Tests create and read actual JFR files to check the custom event schema,
success/failure metadata, disabled-event behavior, and concurrent observer calls.
The failed-step test confirms that synthetic input and exception-message strings
are absent from those custom events.

Startup-redaction tests launch separate JDK 27 processes with
`ProcessBuilder.environment().clear()` and only synthetic `-Dfixture.*` values.
They enable startup system-property events, disable environment-variable events,
and assert that default-sensitive keys are `[REDACTED]`, a safe property remains
visible, and an appended custom filter retains default filtering. The tests do
not record the invoking shell's environment.

The core/Jolt suite and optional Java 25 suite also run on Java 27. Bytecode
minimums stay separate: 17 for core/Jolt, 25 for scoped context, and 27 for this
diagnostics profile. All are non-preview.

[← Back to documentation and versions / Назад к документации и версиям](index.md)
