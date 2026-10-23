package io.github.alex59.pipeline;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Mono;

/**
 * Explicit boundary for blocking operations on an application-owned executor.
 * A virtual-thread-per-task executor is suitable for cooperative blocking I/O;
 * the library never creates or closes an executor. Pipeline concurrency still
 * controls how many records are subscribed, not how much work a cancelled,
 * noncooperative operation can keep doing.
 */
public final class BlockingSteps {
    private static final ScopedValue<ExecutionContext> CURRENT = ScopedValue.newInstance();

    private BlockingSteps() { }

    /**
     * A blocking operation that may throw checked exceptions. Prefer immutable
     * inputs and implement interruption when cancellation should stop the work.
     * @param <I> input type
     * @param <O> non-null result type
     */
    @FunctionalInterface
    public interface Operation<I, O> {
        /**
         * Performs one attempt on the supplied executor.
         * @param input current step input
         * @return a non-null result
         * @throws Exception operation failure, preserved as the step's cause
         */
        O apply(I input) throws Exception;
    }

    /**
     * Adapts blocking work into a cold step. Every subscription submits a fresh
     * task. Cancellation cancels that task with interruption requested; the
     * operation must cooperate, and external effects cannot be rolled back.
     * The scoped execution context is bound only while the operation is running,
     * and is removed before its result is signalled downstream.
     *
     * @param executor application-owned executor; it must not execute blocking
     *                 work inline on the subscribing thread
     * @param operation blocking operation
     * @param <I> input type
     * @param <O> output type
     * @return reusable asynchronous step
     */
    public static <I, O> Step<I, O> call(ExecutorService executor, Operation<I, O> operation) {
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(operation, "operation");
        return (input, context) -> Mono.create(sink -> {
            AtomicBoolean cancelled = new AtomicBoolean();
            FutureTask<O> task = new FutureTask<>(() -> ScopedValue.where(CURRENT, context)
                    .call(() -> Objects.requireNonNull(operation.apply(input), "blocking operation returned null"))) {
                @Override
                protected void done() {
                    if (cancelled.get()) return;
                    switch (state()) {
                        case SUCCESS -> sink.success(resultNow());
                        case FAILED -> sink.error(exceptionNow());
                        case CANCELLED -> { /* Downstream owns cancellation. */ }
                        case RUNNING -> sink.error(new IllegalStateException("Task completion called before a terminal state"));
                    }
                }
            };
            // Register the fully constructed task before handing it to the
            // executor, so cancellation cannot miss an accepted/queued task.
            sink.onCancel(() -> {
                cancelled.set(true);
                task.cancel(true);
            });
            if (cancelled.get()) return;
            try {
                executor.execute(task);
            } catch (RejectedExecutionException failure) {
                task.cancel(false);
                if (!cancelled.get()) sink.error(failure);
            }
        });
    }

    /**
     * Looks up context inside the synchronous dynamic extent of a blocking
     * operation. This is not asynchronous propagation: unrelated threads,
     * Reactor scheduler hops, and downstream signals have no binding from this
     * adapter. Use Reactor context across asynchronous boundaries.
     * @return current blocking operation's context, or empty outside its scope
     */
    public static Optional<ExecutionContext> currentContext() {
        return CURRENT.isBound() ? Optional.of(CURRENT.get()) : Optional.empty();
    }
}
