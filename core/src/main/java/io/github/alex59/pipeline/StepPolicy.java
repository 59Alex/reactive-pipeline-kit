package io.github.alex59.pipeline;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Reliability policy for a single step. Timeout applies to each attempt,
 * excluding the delay between retries. Retry count excludes the first attempt.
 * @param timeout positive attempt timeout
 * @param maxRetries maximum additional attempts
 * @param retryDelay fixed nonnegative delay before another attempt
 * @param retryOn predicate deciding which errors may be retried
 */
public record StepPolicy(Duration timeout, int maxRetries, Duration retryDelay,
                         Predicate<Throwable> retryOn) {
    /** Validates duration and count boundaries. */
    public StepPolicy {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(retryDelay, "retryDelay");
        Objects.requireNonNull(retryOn, "retryOn");
        if (timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("timeout must be positive");
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be nonnegative");
        if (retryDelay.isNegative()) throw new IllegalArgumentException("retryDelay must be nonnegative");
    }

    /**
     * Creates the default policy.
     * @return a 30-second attempt timeout with retries disabled
     */
    public static StepPolicy defaults() {
        return once(Duration.ofSeconds(30));
    }

    /**
     * Creates a policy with retries disabled.
     * @param timeout attempt timeout
     * @return immutable policy
     */
    public static StepPolicy once(Duration timeout) {
        return new StepPolicy(timeout, 0, Duration.ZERO, error -> false);
    }
}
