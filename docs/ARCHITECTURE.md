# Architecture

## Purpose and boundary

The library coordinates asynchronous transformations of individual records in a
host application. It makes concurrency, retry scope, and errors explicit while
leaving business models, persistence, HTTP configuration, and scheduling with the
application. It is an in-process library, not a durable workflow server.

The design develops a step-oriented pipeline concept into a typed Java API.
Instead of a mutable global variable map shared across composite operations,
values move through typed boundaries and execution metadata is created for each
subscription. Retrying a step cannot resubscribe to an earlier step.

## Context and components

```text
Application / input Publisher
    -> Pipeline.process (bounded number of records)
        -> Pipeline.run (one execution identity per subscription)
            -> step A -> step B -> step C
                         |
                         +-> application HttpPort -> external HTTP service
                         +-> optional JoltStep -> isolated JSON tree
        -> output Publisher / downstream subscriber

Step events -> application StepObserver -> application metrics/logging
```

Everything inside `Pipeline` runs in the host process. There are no additional
application containers or library-owned databases. An external service is beyond
the library's consistency boundary. The text diagram above is the complete
runtime context; it does not imply a deployed queue or independent executor.

| Component | Responsibility and state |
|---|---|
| `Pipeline<I,O>` | Immutable composition and step-name validation; holds reusable callbacks |
| `ExecutionContext` | Per-subscription identity and read-only view of Reactor context |
| `StepPolicy` | Attempt timeout, retry count, delay, error predicate |
| `Step` | Application transformation; must return a cold, one-result Mono |
| `StepObserver` | Diagnostics without payload copies; may be called concurrently |
| `HttpPort` | Application-owned remote-call boundary and error classification |
| `BlockingSteps` | Explicit executor boundary for blocking I/O, cancellation through FutureTask, bounded scoped context |
| `JoltStep` | Specification snapshot, built-in operation allowlist, defensive copies |

No mutable attempt counter is stored in the reusable definition. The captured
callbacks, observer, input objects, and objects stored inside Reactor context are
still caller-owned; this library cannot make them immutable or thread-safe.

### Blocking adapter boundary

`BlockingSteps` creates a new `FutureTask` at subscription and registers its
cancellation handler before handing it to the application's executor. Cancellation
during executor handoff therefore still reaches the exact task. A queued task
cancelled before it starts does not invoke the operation. Cancelling an already
running task requests interruption; cooperation remains the operation's duty.

The task binds an immutable `ExecutionContext` with `ScopedValue` only around the
blocking operation. Its `done()` callback uses terminal `Future.State`,
`resultNow()`, or `exceptionNow()` after that binding has ended. It does not block
waiting for a result, and downstream signals do not inherit the scoped binding.
Reactor context is still used for asynchronous propagation. A cancelled operation
that ignores interruption may remain inside its binding until it actually exits.

The application creates and closes the executor. A virtual-thread-per-task
executor allows blocking I/O without making the pipeline core blocking, but it
does not bound submission by itself. Use `process` concurrency limits and do not
interpret cancellation as proof that the physical I/O has stopped.

## Critical flow: enrichment fails once

1. A downstream subscriber subscribes to `run(record)`. The library captures the
   subscriber context and generates a new execution ID.
2. Step A runs and produces its output. Its value becomes step B's input.
3. Step B starts attempt 1, including its timeout. Its remote call fails.
4. The observer receives `FAILED`. If the retry predicate accepts the failure and
   budget remains, Reactor waits the configured delay.
5. Step B starts attempt 2 with the same input reference and execution ID. Step A
   does not execute again. A successful result moves to step C.
6. The downstream subscriber receives the final value. If the retry budget is
   exhausted, it receives `StepFailure` with the last cause instead.

If the subscriber cancels during the retry delay, no subsequent attempt is
started. If it cancels an active attempt, cancellation propagates to the returned
Mono; a noncooperative callback or remote system may still continue its work.

## Invariants

- Every definition has unique, nonblank step names.
- One step emits exactly one non-null value, or fails; empty completions fail.
- Each run subscription has a new identity; all of its attempts share it.
- A retry resubscribes only to the failing step, and its budget is per execution.
- The built-in processing operators subscribe to at most `concurrency` records
  at once, with inner prefetch set to one.
- `INPUT` ordering preserves emitted order, not side-effect execution order.
- Observer failures cannot turn successful business work into a retried step.
- Jolt receives fresh mutable copies and a new transform instance for each
  attempt. Only the allowlisted built-in operations can be selected.
- Blocking operations run only through an explicitly supplied executor. Their
  scoped binding ends on operation exit, including exceptional exit, and is not
  used as a substitute for Reactor context.

## Decisions and tradeoffs

| Decision | Benefit | Cost and reconsideration condition |
|---|---|---|
| Immutable typed Java definitions | Compile-time input/output compatibility and no reflection registry | Runtime JSON workflow configuration needs a separate validated compiler |
| Sequential steps per record, concurrent records | Retry boundaries stay local and understandable | Within-record parallel branches need an explicit application step |
| Fixed retry delay, no implicit retries | Predictable budget and easy virtual-time testing | Large shared deployments may need jitter; provide a separately reviewed policy extension |
| Input-order option | Deterministic downstream sequence | Head-of-line blocking; choose completion order when unnecessary |
| No implicit scheduler changes | Compatible with existing Reactor applications | Blocking calls use an explicit executor adapter; CPU-heavy work chooses an appropriate scheduler explicitly |
| Explicit virtual-thread-capable adapter | Allows blocking drivers at a visible boundary with cooperative cancellation | Noncooperative I/O can outlive cancellation; executor lifecycle belongs to the application |
| Fresh Jolt instance and copied trees | Isolation without locks or assumptions about transform thread safety | Extra allocation and compilation per record; benchmark before optimizing |

## Failure scenarios

| Failure point | Already happened | Observable result | Recovery and verification |
|---|---|---|---|
| Remote response lost after a successful write | External effect may exist | Timeout/error; retry can repeat effect | Application idempotency key; timeout/retry tests verify local scope only |
| Step never completes | Earlier steps may have effects | Attempt timeout cancels current subscription | Bounded retry or final StepFailure; virtual-time timeout/cancel test |
| One of several records fails | Other records may have completed or sent requests | Processing fails and cancels active siblings | App chooses checkpoint/replay; sibling-cancellation test |
| Consumer cancels | Some results may already be emitted | Upstream and active work receive cancellation | No automatic replay or rollback; concurrency/cancellation test |
| Observer throws | Business step may have succeeded | Error sent to Reactor onErrorDropped hook | Repair observer; observer-isolation test ensures no repeated effect |
| Conditional predicate throws | Earlier steps completed, conditional body did not run | StepFailure with zero attempts | Correct input/predicate; no retry of condition |
| Jolt receives unsupported/cyclic data | Earlier steps may have completed | Named transformation error | Validate payload; JSON-copy boundary tests |
| Process terminates | No execution state is durably stored | Active work and progress are lost | Host application provides durable source/checkpoints if needed |
| Executor rejects a blocking task | This blocking attempt has not run | Named StepFailure retaining RejectedExecutionException | Restore executor capacity/lifecycle; rejection test |
| Blocking task ignores interrupt | External I/O may still be active | Reactor observes cancellation/timeout before physical work stops | Application timeout/idempotency; no force-stop guarantee |

The JDK adapter cancellation test and real HTTP exchange establish local client
behavior only. They do not prove that cancellation stops a remote operation.
There is no tested claim of total-memory bounds, arbitrary callback safety, or
durability across process restarts.

## What to know

Use this library to make an in-process pipeline's concurrency and retry boundaries
explicit. Keep step inputs immutable, remote effects idempotent where retries are
enabled, and callbacks nonblocking. Use a durable queue/checkpoint layer outside
this library when records must survive a process crash.
