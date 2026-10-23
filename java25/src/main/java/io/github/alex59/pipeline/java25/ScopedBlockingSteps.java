package io.github.alex59.pipeline.java25;

import io.github.alex59.pipeline.BlockingSteps;
import io.github.alex59.pipeline.ExecutionContext;
import io.github.alex59.pipeline.Step;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Optional Java 25 adapter exposing execution metadata within the synchronous
 * dynamic scope of a blocking operation. Submission, retries and cancellation
 * use the Java 17 core adapter; this class does not create an executor.
 */
public final class ScopedBlockingSteps {
    private static final ScopedValue<ExecutionContext> CURRENT = ScopedValue.newInstance();

    private ScopedBlockingSteps() { }

    /**
     * A blocking operation using scoped context where explicit parameters are
     * inconvenient. Inputs should be immutable and work should cooperate with
     * interruption when cancellation is required.
     * @param <I> input type
     * @param <O> non-null result type
     */
    @FunctionalInterface
    public interface Operation<I, O> {
        /**
         * Performs one attempt inside the scoped binding.
         * @param input step input
         * @return a non-null result
         * @throws Exception operation failure
         */
        O apply(I input) throws Exception;
    }

    /**
     * Binds execution context around each blocking call using the core adapter.
     * The binding ends before success/error is signalled downstream. Cancellation
     * requests interruption; noncooperative work can outlive that cancellation
     * and retain its binding until it actually exits.
     * @param executor application-owned executor, never closed by this library
     * @param operation synchronous operation
     * @param <I> input type
     * @param <O> output type
     * @return reusable step
     */
    public static <I, O> Step<I, O> call(ExecutorService executor, Operation<I, O> operation) {
        Objects.requireNonNull(operation, "operation");
        return BlockingSteps.call(executor, (input, context) ->
                ScopedValue.where(CURRENT, context).call(() -> operation.apply(input)));
    }

    /**
     * Reads context only inside this adapter's synchronous operation scope.
     * This binding does not automatically propagate across Reactor scheduler
     * hops or unrelated executor submissions; use Reactor context for those.
     * @return bound context or empty outside the operation's dynamic scope
     */
    public static Optional<ExecutionContext> currentContext() {
        return CURRENT.isBound() ? Optional.of(CURRENT.get()) : Optional.empty();
    }
}
