package io.github.alex59.pipeline;

import java.util.Objects;
import java.util.UUID;
import reactor.util.context.ContextView;

/**
 * Immutable metadata created for each subscription to a single input's run.
 * Caller-owned values in Reactor context are not copied or made thread-safe.
 * @param executionId identity shared by all steps and retries of this run
 * @param subscriberContext Reactor subscriber context captured at subscription
 */
public record ExecutionContext(UUID executionId, ContextView subscriberContext) {
    /**
     * Validates the execution metadata.
     * @param executionId unique execution identity
     * @param subscriberContext captured subscriber context
     */
    public ExecutionContext {
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(subscriberContext, "subscriberContext");
    }
}
