package io.github.alex59.pipeline;

import reactor.core.publisher.Mono;

/**
 * One asynchronous transformation. Called anew for every retry attempt.
 * Implementations must return a cold Mono emitting exactly one non-null value.
 * Do not mutate the input when a retry might need the original value.
 *
 * @param <I> input type
 * @param <O> output type
 */
@FunctionalInterface
public interface Step<I, O> {
    /**
     * Builds the work for one attempt; no blocking work should happen here.
     * @param input current value
     * @param context immutable execution identity and subscriber context
     * @return one result, or an error
     */
    Mono<O> apply(I input, ExecutionContext context);
}
