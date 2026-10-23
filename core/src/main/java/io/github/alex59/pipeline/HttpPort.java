package io.github.alex59.pipeline;

import reactor.core.publisher.Mono;

/**
 * Application-owned asynchronous HTTP boundary, independent of an HTTP client.
 * Implementations should propagate cancellation and return a cold Mono.
 * @param <Q> request representation
 * @param <R> response representation
 */
@FunctionalInterface
public interface HttpPort<Q, R> {
    /**
     * Performs one HTTP attempt.
     * @param request request
     * @param context current execution metadata
     * @return one response or error
     */
    Mono<R> exchange(Q request, ExecutionContext context);

    /**
     * Adapts this port to a pipeline step.
     * @return a step backed by this port
     */
    default Step<Q, R> asStep() { return this::exchange; }
}
